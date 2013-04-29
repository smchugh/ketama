/*
* Copyright (c) 2007, Last.fm, All rights reserved.
* Richard Jones <rj@last.fm>
* Christian Muehlhaeuser <chris@last.fm>
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*     * Redistributions of source code must retain the above copyright
*       notice, this list of conditions and the following disclaimer.
*     * Redistributions in binary form must reproduce the above copyright
*       notice, this list of conditions and the following disclaimer in the
*       documentation and/or other materials provided with the distribution.
*     * Neither the name of the Last.fm Limited nor the
*       names of its contributors may be used to endorse or promote products
*       derived from this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY Last.fm ``AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL Last.fm BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include "ketama.h"
#include "ketama_config.h"

#if defined(ENABLE_FNV_HASH)
#include "fnv.h"
#else
#include "md5.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <time.h>           /* for reading last time modification   */
#include <unistd.h>         /* needed for usleep                    */
#include <math.h>           /* floor & floorf                       */
#include <sys/stat.h>       /* various type definitions             */
#include <sys/shm.h>        /* shared memory functions and structs  */
#include <syslog.h>

#define HASH_COUNT POINTS_PER_SERVER / POINTS_PER_HASH


char k_error[255] = "";

int num_sem_ids = 0;
int sem_ids_size = 10;
int* sem_ids = NULL;

int num_resources = 0;
int shm_resources_size = 10;
continuum_resource* shm_resources = NULL;

static void syslog1(int lev, char *msg)
{
  syslog(lev, "%s", msg);
}

static void fatal_exit()
{
  exit(1);
}

static void
init_shm_resource_tracker() {
    shm_resources = (continuum_resource*) malloc( sizeof(continuum_resource) * shm_resources_size );
    if (shm_resources == NULL) {
        syslog1( LOG_INFO, "Ketama: Cannot malloc shm resource tracker.\n" );
        fatal_exit();
    }
}

static void
track_shm_resource(continuum_resource resource) {
    int i, indx = -1;

    // find the resource and update it
    for (i = 0; i < num_resources; i++) {
        if (shm_resources[i].key == resource.key) {
            indx = i;
            shm_resources[i].data = resource.data;
            shm_resources[i].shmid = resource.shmid;
        }
    }

    // if we didn't find it, create a new one
    if (indx == -1) {
        if (num_resources == shm_resources_size) {
            shm_resources = (continuum_resource*) realloc( shm_resources, sizeof(continuum_resource) * shm_resources_size * 2 );
            if (shm_resources == NULL) {
                syslog1( LOG_INFO, "Ketama: Cannot realloc shm resource tracker.\n" );
                fatal_exit();
            }

            shm_resources_size *= 2;
        }

        shm_resources[num_resources] = resource;
        num_resources++;
    }
}

/** \brief Retrieves an existing or new resource.
  * \param key Key used to attach to shared memory segment.
  * \param cont Pointer to the continuum resource that will be filled with the location of a valid resource.
  * \return 1 on success, 0 for failure */
static int
get_shm_resource(key_t key, ketama_continuum cont) {
    int shmid, i, indx = -1;

    // find the resource
    for (i = 0; i < num_resources; i++) {
        if (shm_resources[i].key == key) {
            // if the continuum pointer is invalid don't count this as a found resource
            if (shm_resources[i].data != NULL) {
                indx = i;
            }
        }
    }

    // send back the existing resource if it exists
    if (indx != -1) {
        *cont = shm_resources[indx];
        return 1;
    }

    // attempt to obtain the shared memory ID assigned to this key, and create a segment if it doesn't exist
    shmid = shmget( key, MC_SHMSIZE, 0644 | IPC_CREAT );
    if ( shmid == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: get_shm_resource failed to get valid shared memory segment with errno: %d.\n", errno );
        syslog1( LOG_INFO, k_error );
        cont = NULL;
        return 0;
    }

    // attempt to create an attachment to the shared memory segment
    cont->data = (continuum*) shmat( shmid, (void *)0, SHM_RDONLY );
    if ( cont->data == (void *)(-1) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: get_shm_resource failed to attach read-only shared memory segment with errno: %d.\n", errno );
        syslog1( LOG_INFO, k_error  );
        cont = NULL;
        return 0;
    }
    cont->shmid = shmid;
    cont->key = key;

    return 1;
}

static int
ketama_shmdt(void* data)
{
    int i, result;

    // reset the tracked resource to dummy values
    for (i = 0; i < num_resources; i++) {
        if (shm_resources[i].data == data) {
            shm_resources[i].data = NULL;
            shm_resources[i].shmid = -1;
        }
    }

#ifdef SOLARIS
    result = shmdt( (char *) data );
#else
    result = shmdt( data );
#endif

    return result;
}

static void
init_sem_id_tracker() {
    sem_ids = (int*) malloc( sizeof(int) * sem_ids_size );
    if (sem_ids == NULL) {
        syslog1( LOG_INFO, "Ketama: Cannot malloc semaphore tracker.\n" );
        fatal_exit();
    }
}

static void
track_sem_id(int semid) {
    int i, indx = -1;

    // find the semaphore
    for (i = 0; i < num_sem_ids; i++) {
        if (sem_ids[i] == semid) {
            indx = i;
        }
    }

    if (indx == -1) {
        if (num_sem_ids == sem_ids_size) {
            sem_ids = (int*) realloc( sem_ids, sizeof(int) * sem_ids_size * 2 );
            if (sem_ids == NULL) {
                syslog1( LOG_INFO, "Ketama: Cannot realloc semids.\n" );
                fatal_exit();
            }

            sem_ids_size *= 2;
        }

        sem_ids[num_sem_ids] = semid;
        num_sem_ids++;
    }
}

/** \brief Locks the semaphore.
  * \param sem_set_id The semaphore handle that you want to lock. */
static void
ketama_sem_lock( int sem_set_id )
{
    union semun sem_val;
    sem_val.val = 2;
    semctl( sem_set_id, 0, SETVAL, sem_val );
}


/** \brief Unlocks the semaphore.
  * \param sem_set_id The semaphore handle that you want to unlock. */
static void
ketama_sem_unlock( int sem_set_id )
{
    union semun sem_val;
    sem_val.val = 1;
    semctl( sem_set_id, 0, SETVAL, sem_val );
}


/** \brief Initialize a semaphore.
  * \param key Semaphore key to use.
  * \return The freshly allocated semaphore handle. */
static int
ketama_sem_init( key_t key )
{
    if (sem_ids == NULL) {
        init_sem_id_tracker();
        syslog1( LOG_INFO, "Semaphore tracker initiated.\n" );
    }

    int sem_set_id;

    sem_set_id = semget( key, 1, IPC_CREAT | 0666 );

    if ( sem_set_id == -1 )
    {
        snprintf( k_error, sizeof(k_error), "Ketama: Could not open semaphore!\n" );
        syslog1( LOG_INFO, k_error );
        return 0;
    }

    // track the semaphore if it exists
    track_sem_id(sem_set_id);

    return sem_set_id;
}

/** \brief Locks the semaphore only after it is unlocked by another process.
  * \param sem_set_id The semaphore handle that you want to lock. */
static void
ketama_sem_safely_lock( int sem_set_id )
{
    int sanity = 0;
    while ( semctl( sem_set_id, 0, GETVAL, 0 ) == 2 )
    {
        // wait for the continuum creator to finish, but don't block others
        usleep( 5 );

        // if we are waiting for > 1 second, take drastic action:
        if(++sanity > 200000)
        {
            usleep( rand()%50000 );
            ketama_sem_unlock( sem_set_id );
            break;
        }
    }

    ketama_sem_lock( sem_set_id );
}


/* ketama.h does not expose this function */
#if !defined(ENABLE_FNV_HASH)
void
ketama_md5_digest( char* inString, unsigned char *md5pword )
{
    md5_state_t md5state;

    md5_init( &md5state );
    md5_append( &md5state, (unsigned char *)inString, strlen( inString ) );
    md5_finish( &md5state, md5pword );
}
#endif


/** \brief Retrieve the modification time of a file.
  * \param filename The full path to the file.
  * \return The timestamp of the latest modification to the file. */
static time_t
file_modtime( char* filename )
{
    struct stat attrib;

    stat( filename, &attrib );

    return attrib.st_mtime;
}

/** \brief Retrieve the "unique" key to be used for the shared memory and semaphores
  * \param filename The full path to the file, or user-defined key of the form key:0x12345678.
  * \return The key.
  * \todo Ideally ftok wouldn't be used and instead a heashing function would be used to set the upper
  *       16bits of the key ro some hash when the key is made from a string and the bottom 16bits when
  *       the key is made from a path, to prevent key collisions between user-defined keys and paths*/
static key_t
get_key( char* filename, time_t* fmodtime )
{
    key_t key;
    if (strncmp(filename, "key:", 4) == 0) {
        unsigned int key_val = -1;
        sscanf(filename, "key:%x", &key_val );
        *fmodtime = 0;
        key = (key_t) key_val;
    } else {
        key = ftok(filename, 'R');
        *fmodtime = file_modtime( filename );
    }

    return key;
}



/** \brief Retrieve a serverinfo struct for one a sever definition.
  * \param line The entire server definition in plain-text. */
static void
read_server_line( char* line, serverinfo* server )
{
    char* delim = "\t ";
    server->memory = 0;
    char* saveptr = line;

    char* tok = strtok_r( line, delim, &saveptr );

    char* endptr = NULL;

    snprintf( server->addr, sizeof(server->addr), "%s", tok );

    tok = strtok_r( NULL, delim, &saveptr );

    /* We do not check for a NULL return earlier because strtok will
     * always return at least the first token; hence never return NULL.
     */
    if ( tok == NULL ) {
        snprintf( k_error, sizeof(k_error), "Ketama: Unable to find delimiter.\n" );
        syslog1( LOG_INFO, k_error );
        server->memory = 0;
    } else {
        errno = 0;
        server->memory = strtol( tok, &endptr, 10 );
        if ( errno == ERANGE || endptr == tok ) {
            snprintf( k_error, sizeof(k_error), "Ketama: Invalid memory value.\n" );
            syslog1( LOG_INFO, k_error );
            server->memory = 0;
        }
    }
}


/** \brief Retrieve all server definitions from a file.
  * \param filename The full path to the file which contains the server definitions.
  * \param count The value of this pointer will be set to the amount of servers which could be parsed.
  * \param memory The value of this pointer will be set to the total amount of allocated memory across all servers.
  * \return A serverinfo array, containing all servers that could be parsed from the given file. */
static serverinfo*
read_server_definitions( char* filename, int* count, unsigned long* memory )
{
    serverinfo* slist = NULL;
    unsigned int lineno = 0;
    int numservers = 0;
    unsigned long memtotal = 0;
    serverinfo server;

    FILE* fi = fopen( filename, "r" );
    while ( fi && !feof( fi ) )
    {
        char sline[128];

        char* line = fgets( sline, sizeof(sline), fi );
        if (line == NULL) {
            break;
        }
        lineno++;

        if ( strlen( sline ) < 2 || sline[0] == '#' )
            continue;

        read_server_line( sline, &server );
        if ( server.memory > 0 && strlen( server.addr ) > 0 ) {
            slist = (serverinfo*)realloc( slist, sizeof( serverinfo ) * ( numservers + 1 ) );
            memcpy( &slist[numservers], &server, sizeof( serverinfo ) );
            numservers++;
            memtotal += server.memory;
        } else {
            // This kind of tells the parent code that "there were servers but not really"
            *count = 1;
            free( slist );
            snprintf( k_error, sizeof(k_error), "Ketama: Server %s with memory %lu could not be loaded.\n", server.addr, server.memory );
            syslog1( LOG_INFO, k_error );
            if (fi != NULL) {
                fclose( fi );
            }
            return 0;
        }
    }

    if ( !fi ) {
        snprintf( k_error, sizeof(k_error), "Ketama: File %s doesn't exist!", filename );
        syslog1( LOG_INFO, k_error );

        *count = 0;
        return 0;
    } else {
        fclose( fi );
    }

    // sort the server list
    qsort( (void *) slist, numservers, sizeof( serverinfo ), (compfn)serverinfo_compare );

    *count = numservers;
    *memory = memtotal;
    return slist;
}

/** \brief Adds a server to the continuum struct
  * \param addr The address of the server that you want to add.
  * \param newmemory The amount of allocated memory from this server to be added to the cluster
  * \param cont Pointer to the continuum which we will refresh.
  * \param count The value of this pointer will be set to the amount of servers which could be parsed.
  * \param memory The value of this pointer will be set to the total amount of allocated memory across all servers.
  * \param newslist Pointer to an uninitialized serverinfo pointer that will be allocated in this function (if the server isn't found) and passed back by reference
  * \return 0 on failure, 1 on success. */
int
add_serverinfo( char* addr, unsigned long newmemory, continuum* cont, int* count, unsigned long* memory, serverinfo **newslist)
{
    int i, numservers, indx = -1;
    unsigned long memtotal;
    serverinfo newserver;
    newserver.memory = 0;

    // get the current number of servers and available total memory
    numservers = cont->numservers;
    memtotal = cont->memtotal;

    // search for the server to make sure we don't add duplicates
    for (i = 0; i < numservers; i++) {
        if (strcmp(addr, cont->slist[i].addr) == 0) {
            indx = i;
        }
    }

    if (indx != -1) {
        return 0;
    }

    // populate the new server struct
    snprintf( newserver.addr, sizeof(newserver.addr), "%s", addr );
    newserver.memory = newmemory;

    // add the server to the list
    *newslist = (serverinfo*) malloc( sizeof( serverinfo ) * ( numservers + 1 ) );
    for (i = 0; i < numservers; i++) {
        memcpy( &(*newslist)[i], &cont->slist[i], sizeof(serverinfo) );
    }
    memcpy( &(*newslist)[numservers], &newserver, sizeof(serverinfo) );
    numservers++;
    memtotal += newmemory;

    // sort the server list
    qsort( (void *) *newslist, numservers, sizeof( serverinfo ), (compfn)serverinfo_compare );

    snprintf( k_error, sizeof(k_error), "Ketama: %s was added.\n", addr);
    syslog1( LOG_INFO, k_error );

    *count = numservers;
    *memory = memtotal;

    return 1;
}

/** \brief Removes a server from the continuum struct
  * \param addr The address of the server that you want to remove.
  * \param cont Pointer to the continuum which we will refresh.
  * \param count The value of this pointer will be set to the amount of servers which could be parsed.
  * \param memory The value of this pointer will be set to the total amount of allocated memory across all servers.
  * \param newslist Pointer to an uninitialized serverinfo pointer that will be allocated in this function (if the server is found) and passed back by reference
  * \return 0 on failure, 1 on success. */
int
remove_serverinfo( char* addr, continuum* cont, int* count, unsigned long* memory, serverinfo **newslist)
{
    int i, j, numservers, rm_indx = -1;
    unsigned long memtotal, oldmemory = 0;

    // get the current number of servers and available total memory
    numservers = cont->numservers;
    memtotal = cont->memtotal;

    for (i = 0; i < numservers; i++) {
        if (strcmp(addr, cont->slist[i].addr) == 0) {
            rm_indx = i;
        }
    }
    // if we didn't find the server, don't remove anything and throw a warning
    if (rm_indx == -1) {
        return 0;
    }

    *newslist = (serverinfo*) malloc( sizeof( serverinfo ) * ( numservers - 1 ) );
    for (i = 0, j = 0; i < numservers; i++) {
        if (i != rm_indx) {
            memcpy( &(*newslist)[j], &cont->slist[i], sizeof(serverinfo) );
            j++;
        } else {
            oldmemory = cont->slist[i].memory;
        }
    }
    numservers--;
    memtotal -= oldmemory;

    // sort the server list
    qsort( (void *) *newslist, numservers, sizeof( serverinfo ), (compfn)serverinfo_compare );
    snprintf( k_error, sizeof(k_error), "Ketama: %s was removed.\n", addr);
    syslog1( LOG_INFO, k_error );

    *count = numservers;
    *memory = memtotal;

    return 1;
}


unsigned int
ketama_hashi( char* inString )
{
#if defined(ENABLE_FNV_HASH)
    return fnv_32a_str( inString, FNV1_32_INIT );
#else
    unsigned char digest[16];

    ketama_md5_digest( inString, digest );
    return (unsigned int)(( digest[3] << 24 )
                        | ( digest[2] << 16 )
                        | ( digest[1] <<  8 )
                        |   digest[0] );
#endif
}


mcs*
ketama_get_server( char* key, ketama_continuum cont )
{
    unsigned int h = ketama_hashi( key );
    int lowp = 0, midp, highp;
    unsigned int midval, midval1;

    // verify that a valid resource was passed in before getting its key
    if (cont == NULL) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_get_server passed illegal pointer to a continuum resource.\n" );
        syslog1( LOG_INFO, k_error );
        return NULL;
    }
    key_t fkey = cont->key;

    // try to find an existsing resource with a shared memory segment in the array of tracked resources, or create a new attachment resource
    if ( !get_shm_resource(fkey, cont) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_get_server failed to get a valid resource.\n" );
        syslog1( LOG_INFO, k_error  );
        return NULL;
    }

    // divide and conquer array search to find server with next biggest
    // point after what this key hashes to
    highp = cont->data->numpoints;
    while ( 1 ) {
        midp = ( lowp+highp ) >> 1;

        if ( midp == cont->data->numpoints )
            return &cont->data->array[0]; // if at the end, roll back to zeroth

        midval = cont->data->array[midp].point;
        midval1 = midp == 0 ? 0 : cont->data->array[midp-1].point;

        if ( h <= midval && h > midval1 )
            return &cont->data->array[midp];

        if ( midval < h )
            lowp = ++midp;
        else
            highp = --midp;

        if ( lowp > highp )
            return &cont->data->array[0];
    }
}

int
ketama_get_server_count( ketama_continuum cont )
{
    // verify that a valid resource was passed in before getting its key
    if (cont == NULL) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_get_server_count passed illegal pointer to a continuum resource.\n" );
        syslog1( LOG_INFO, k_error );
        return -1;
    }
    key_t key = cont->key;

    // try to find an existsing resource with a shared memory segment in the array of tracked resources, or create a new attachment resource
    if ( !get_shm_resource(key, cont) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_get_server_count failed to get a valid resource.\n" );
        syslog1( LOG_INFO, k_error  );
        return -1;
    }

    return cont->data->numservers;
}

/** \brief Loads the continuum of servers (each server as many points on a circle).
  * \param key Shared memory key for storing the newly created continuum.
  * \param slist The address of the list of servers that your building the continuum from.
  * \param numservers Number of servers available
  * \param memory Amount of memory available accross all servers
  * \param fmodtime File modtime
  * \return 0 on failure, 1 on success. */
int
load_continuum(key_t key, serverinfo* slist, int numservers, unsigned long memory, time_t fmodtime)
{
    int maxpoints = POINTS_PER_SERVER * numservers; // maximum number of ring points (HASH_COUNT * POINTS_PER_HASH * numservers)

    // Continuum will hold one mcs for each point on the circle:
    mcs *ring = (mcs*) malloc( maxpoints * sizeof(mcs) );
    int i, k, numpoints = 0;

    // buildup the continuum ring
    for( i = 0; i < numservers; i++ )
    {
        float ratio = (float) slist[i].memory / (float)memory; // ratio of hashes for this server to total hashes
        int numhashes = floorf( ratio * HASH_COUNT * (float)numservers ); // number of hashes for this server
#ifdef DEBUG
        int percent = floorf( ratio * 100.0 ); // percent of total hashes linked to this sever
        syslog( LOG_INFO, "Ketama: Server no. %d: %s (mem: %lu = %u%% or %d of %d)\n",
            i, slist[i].addr, slist[i].memory, percent, numhashes * POINTS_PER_HASH, HASH_COUNT * numservers * POINTS_PER_HASH );
#endif

#if defined(ENABLE_FNV_HASH)
        Fnv32_t hval = FNV1_32_INIT;
#endif

        // create the points on the ring for this server
        for( k = 0; k < numhashes; k++ )
        {
#if defined(ENABLE_FNV_HASH)
            hval = fnv_32a_str(slist[i].addr, hval);
            ring[numpoints].point = hval;
            snprintf( ring[numpoints].ip, sizeof(ring[numpoints].ip), "%s", slist[i].addr);
            numpoints++;
#else
            char ss[30];
            unsigned char digest[16];

            snprintf( ss, sizeof(ss), "%s-%d", slist[i].addr, k );
            ketama_md5_digest( ss, digest );

            // Use successive 4-bytes from hash as numbers for the points on the circle:
            int h;
            for( h = 0; h < POINTS_PER_HASH; h++ )
            {
                ring[numpoints].point = ( digest[3+h*4] << 24 )
                                      | ( digest[2+h*4] << 16 )
                                      | ( digest[1+h*4] <<  8 )
                                      |   digest[h*4];

                snprintf( ring[numpoints].ip, sizeof(ring[numpoints].ip), "%s", slist[i].addr);
                numpoints++;
                if (numpoints > maxpoints) {
                    snprintf( k_error, sizeof(k_error), "Ketama: load_continuum tried to exceed mcs array bounds.\n" );
                    syslog1( LOG_INFO, k_error );
                    free(ring);
                    free(slist);
                    return 0;
                }
            }
#endif
        }
    }

    return reconstruct_continuum(key, slist, numservers, ring, numpoints, memory, fmodtime);
}

/** \brief Purges the continuum of the points on the circle for a specified server
  * \param addr The address of the server that you want to remove points linked to.
  * \param cont Pointer to the continuum which we will refresh.
  * \param key Shared memory key for storing the newly created continuum.
  * \param slist The address of the list of servers that your building the continuum from.
  * \param numservers Number of servers available
  * \param memory Amount of memory available accross all servers
  * \param fmodtime File modtime
  * \return 0 on failure, 1 on success. */
int
remove_server_from_continuum(char* addr, continuum* cont, key_t key, serverinfo* slist, int numservers, unsigned long memory, time_t fmodtime)
{
    int maxpoints = POINTS_PER_SERVER * numservers; // maximum number of ring points (HASH_COUNT * POINTS_PER_HASH * numservers)
    int allpoints = cont->numpoints; // total number of points in the ring prior to server removal

    // Continuum will hold one mcs for each point on the circle:
    mcs *ring = (mcs*) malloc( allpoints * sizeof(mcs) );
    int i, k, numpoints = 0;

    // buildup the new continuum ring
    for( i = 0; i < allpoints; i++ )
    {
        // if this point is not linked to the server to purge, add it to the new ring
        if (strcmp(addr, cont->array[i].ip) != 0) {
            memcpy( &ring[numpoints], &cont->array[i], sizeof(mcs) );
            numpoints++;
            if (numpoints > maxpoints) {
                snprintf( k_error, sizeof(k_error), "Ketama: remove_server_from_continuum tried to exceed mcs array bounds.\n" );
                syslog1( LOG_INFO, k_error );
                free(ring);
                free(slist);
                return 0;
            }
        }
    }

    return reconstruct_continuum(key, slist, numservers, ring, numpoints, memory, fmodtime);
}

/** \brief Adds a specified server to the continuum that has just been added to the server list
  * \param addr The address of the server that you want to add points for.
  * \param newmemory The amount of allocated memory from the new server to be added to the cluster
  * \param cont Pointer to the continuum which we will refresh.
  * \param key Shared memory key for storing the newly created continuum.
  * \param slist The address of the list of servers that your building the continuum from.
  * \param numservers Number of servers available
  * \param memory Amount of memory available accross all servers
  * \param fmodtime File modtime
  * \return 0 on failure, 1 on success. */
int
add_server_to_continuum(char* addr, unsigned long newmemory, continuum* cont, key_t key, serverinfo* slist, int numservers, unsigned long memory, time_t fmodtime)
{
    int maxpoints = POINTS_PER_SERVER * numservers; // maximum number of ring points (HASH_COUNT * POINTS_PER_HASH * numservers)
    int numoldpoints = cont->numpoints; // total number of points in the ring prior to adding the server

    // Continuum will hold one mcs for each point on the circle:
    mcs *ring = (mcs*) malloc( maxpoints * sizeof(mcs) );
    int i, k, indx = -1, numpoints = 0;

    // determine the number of hashes to create
    float ratio = (float)newmemory / (float)memory; // ratio of hashes for this server to total hashes
    int numhashes = floorf( ratio * HASH_COUNT * (float)numservers ); // number of hashes for this server
    //given unevenly distributed memory we need to limit number of hashes
    int max_hashcount = POINTS_PER_SERVER / POINTS_PER_HASH;
    if (numhashes > max_hashcount) {
        numhashes = max_hashcount;
    }
#ifdef DEBUG
    int percent = floorf( ratio * 100.0 ); // percent of total hashes linked to this sever
    syslog( LOG_INFO, "Ketama: Server no. %d: %s (mem: %lu = %u%% or %d of %d)\n",
        i, addr, newmemory, percent, numhashes * POINTS_PER_HASH, HASH_COUNT * numservers * POINTS_PER_HASH );
#endif

#if defined(ENABLE_FNV_HASH)
    Fnv32_t hval = FNV1_32_INIT;
#endif

    // create the points on the ring for the new server
    for( k = 0; k < numhashes; k++ )
    {
#if defined(ENABLE_FNV_HASH)
        hval = fnv_32a_str(addr, hval);
        ring[numpoints].point = hval;
        snprintf( ring[numpoints].ip, sizeof(ring[numpoints].ip), "%s", addr);
        numpoints++;
        if (numpoints > maxpoints) {
       	   snprintf( k_error, sizeof(k_error), "Ketama: add_server_to_continuum tried to exceed mcs array bounds.\n" );
           syslog1( LOG_INFO, k_error );
           free(ring);
           free(slist);
           return 0;
        }
#else
        char ss[30];
        unsigned char digest[16];

        snprintf( ss, sizeof(ss), "%s-%d", addr, k );
        ketama_md5_digest( ss, digest );

        // Use successive 4-bytes from hash as numbers for the points on the circle:
        int h;
        for( h = 0; h < POINTS_PER_HASH; h++ )
        {
            ring[numpoints].point = ( digest[3+h*4] << 24 )
                                  | ( digest[2+h*4] << 16 )
                                  | ( digest[1+h*4] <<  8 )
                                  |   digest[h*4];

            snprintf( ring[numpoints].ip, sizeof(ring[numpoints].ip), "%s", addr);
            numpoints++;
            if (numpoints > maxpoints) {
                snprintf( k_error, sizeof(k_error), "Ketama: add_server_to_continuum tried to exceed mcs array bounds.\n" );
                syslog1( LOG_INFO, k_error );
                free(ring);
                free(slist);
                return 0;
            }
        }
#endif
    }

    // append the points on the current ring (will be sorted in the reconstruction phase)
    for( i = 0; i < numoldpoints; i++ )
    {
        memcpy( &ring[numpoints], &cont->array[i], sizeof(mcs) );
        numpoints++;
        if (numpoints > maxpoints) {
            snprintf( k_error, sizeof(k_error), "Ketama: add_server_to_continuum tried to exceed mcs array bounds.\n" );
            syslog1( LOG_INFO, k_error );
            free(ring);
            free(slist);
            return 0;
        }
    }

    // reconstruct the continuum
    return reconstruct_continuum(key, slist, numservers, ring, numpoints, memory, fmodtime);
}

int
reconstruct_continuum(key_t key, serverinfo* slist, int numservers, mcs* ring, int numpoints, unsigned long memory, time_t fmodtime) {
    int shmid, sem_set_id;
    continuum* data;  // Pointer to shmem location

    // sort the ring in ascending order of "point"
    qsort( (void*) ring, numpoints, sizeof( mcs ), (compfn)ketama_compare );

    // attempt to obtain the shared memory ID assigned to this key, and create a segment if it doesn't exist
    shmid = shmget( key, MC_SHMSIZE, 0644 | IPC_CREAT );
    if ( shmid == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: reconstruct_continuum failed to get valid shared memory segment with errno: %d.\n", errno );
        syslog1( LOG_INFO, k_error );
        return 0;
    }

    // lock the semaphore to prevent other writing to the shared memory segment
    sem_set_id = ketama_sem_init( key );
    ketama_sem_safely_lock( sem_set_id );
    int invalid_write = 0;

    // create an attachment in virtual memory to the shared memory segment, and failout if that an error is returned
    data = (continuum*) shmat( shmid, (void *)0, 0 );
    if ( data == (void *)(-1) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: reconstruct_continuum failed to attach writable shared memory segment with errno: %d.\n", errno );
        invalid_write = 1;
    }

    // verify that the mcs array can hold this many points
    if ( sizeof(data->array) < sizeof(mcs) * numpoints ) {
        snprintf( k_error, sizeof(k_error), "Ketama: reconstruct_continuum tried to exceed size of mcs array.\n" );
        ketama_shmdt(data);
        invalid_write = 1;
    }

    // verify that the serverinfo array can hold this many servers
    if ( sizeof(data->slist) < sizeof(serverinfo) * numservers ) {
        snprintf( k_error, sizeof(k_error), "Ketama: reconstruct_continuum tried to exceed size of servers array.\n" );
        ketama_shmdt(data);
        invalid_write = 1;
    }

    // gracefully failout if any of the preceeding checks discovered errors
    if (invalid_write) {
        syslog1( LOG_INFO, k_error );
        free(ring);
        free(slist);
        ketama_sem_unlock( sem_set_id );
        return 0;
    }

    // record the new data into the shared memory segment
    data->numpoints = numpoints;
    data->numservers = numservers;
    data->memtotal = memory;
    data->fmodtime = fmodtime;
    memcpy( data->array, ring, sizeof( mcs ) * numpoints );
    memcpy( data->slist, slist, sizeof( serverinfo ) * numservers );
    syslog( LOG_INFO, "Ketama: copied mcs array into %p from %p and slist into %p from %p.\n", data->array, ring, data->slist, slist );

    data->cont_version++;
    data->cont_modtime = time(NULL);

    // We detatch here because we will re-attach in read-only mode to actually use it.
    if (ketama_shmdt(data) == -1) {
        snprintf( k_error, sizeof(k_error), "Ketama: reconstruct_continuum failed to detatch from shared memory with errno: %d.\n", errno );
        syslog1( LOG_INFO, k_error );
    }
    // unlock the semaphore
    ketama_sem_unlock( sem_set_id );

    // clean up
    free(ring);
    free(slist);

    return 1;
}

/** \brief Rolls the ketama, or checks for propper creation based on passed in flag
  * \param contptr The value of this pointer will contain the retrieved continuum.
  * \param filename The server-definition file which defines our continuum.
  * \param roller_flag 0 to allow creation if continuum is stale or non-existent,
  *                    1 to only allow a check for a valid continuum
  * \return 0 on failure, 1 on success. */
static int ketama_roller( ketama_continuum* contptr, char* filename, int roller_flag );
// need this declared here, but it will be defined later on


/** \brief Generates the continuum of servers (each server as many points on a circle).
  * \param key Shared memory key for storing the newly created continuum.
  * \param filename Server definition file, which will be parsed to create this continuum.
  * \param contptr The value of this pointer will contain the retrieved continuum resource.
  * \return 0 on failure, 1 on success. */
static int
ketama_create_continuum(key_t key, char* filename, ketama_continuum* contptr)
{
    int shmid, sem_set_id;
    int numservers = 0;
    unsigned long memory;
    serverinfo* slist;
    continuum* data;

    // if filename is not a user-specific key, init a shared memory segment from the pathname, if it's valid
    if (strncmp(filename, "key:", 4) != 0) {
        // get the list of servers from the provided file
        slist = read_server_definitions( filename, &numservers, &memory );

        // Check numservers first; if it is zero then there is no error message and we need to set one.
        if ( numservers < 1 ) {
            snprintf( k_error, sizeof(k_error), "Ketama: No valid server definitions in file %s", filename );
            syslog1( LOG_INFO, k_error );
            return 0;
        } else if ( slist == NULL ) {
            // read_server_definitions must've set error message.
            return 0;
        }

        // log the sucessful file read
        syslog( LOG_INFO, "Ketama: Server definitions read: %u servers, total memory: %lu.\n", numservers, memory );

        time_t fmodtime = file_modtime( filename );

        // attempt to load the continuum and log an error if this fails
        if ( !load_continuum( key, slist, numservers, memory, fmodtime ) ) {
            snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed to load the continuum.\n" );
            syslog1( LOG_INFO, k_error );
            return 0;
        }
    }

    // attempt to obtain the shared memory ID assigned to this key, and create a segment if it doesn't exist
    shmid = shmget( key, MC_SHMSIZE, 0644 | IPC_CREAT );
    if ( shmid == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed to get valid shared memory segment with errno: %d.\n", errno );
        syslog1( LOG_INFO, k_error );
        return 0;
    }

    // lock the semaphore to prevent other writing to the shared memory segment
    sem_set_id = ketama_sem_init( key );
    ketama_sem_safely_lock( sem_set_id );

    // create an attachment in virtual memory to the shared memory segment, and failout if that an error is returned
    data = (continuum*) shmat( shmid, (void *)0, 0 );
    if ( data == (void *)(-1) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed to attach writable shared memory segment with errno: %d.\n", errno );
        syslog1( LOG_INFO, k_error  );
        ketama_sem_unlock( sem_set_id );
        return 0;
    }

    // if filename is not a path, init an empty shared memory segment, otherwise just record the filename
    if (strncmp(filename, "key:", 4) == 0) {
        data->fmodtime = 0;
        data->cont_version = 1;
        data->cont_modtime = time(NULL);
    }
    snprintf(data->cont_filename, sizeof(data->cont_filename), "%s", filename);

    // attempt to detach the shared memory segment and throw an error if one is received
    if (ketama_shmdt(data) == -1) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed to detatch from shared memory with errno: %d.\n", errno );
        syslog1( LOG_INFO, k_error );
    }

    // unlock the semaphore
    ketama_sem_unlock( sem_set_id );

    // rerun ketama_roller with the roller flag on to validate the loaded continuum
    return ketama_roller( contptr, filename, 1);
}

int
ketama_add_server( char* addr, unsigned long newmemory, ketama_continuum cont)
{
    key_t key;
    int numservers = 0;
    unsigned long memory;
    serverinfo* slist;
    time_t fmodtime;

    // verify that a valid resource was passed in before getting its key
    if (cont == NULL) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_add_server passed illegal pointer to a continuum resource.\n" );
        syslog1( LOG_INFO, k_error );
        return 0;
    }
    key = cont->key;

    // try to find an existsing resource with a shared memory segment in the array of tracked resources, or create a new attachment resource
    if ( !get_shm_resource(key, cont) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_add_server failed to get a valid resource.\n" );
        syslog1( LOG_INFO, k_error  );
        return 0;
    }

    // get the new server list
    if (add_serverinfo( addr, newmemory, cont->data, &numservers, &memory, &slist)) {
        // get the shared memory segment key
        key = get_key( cont->data->cont_filename, &fmodtime );
        if ( key == -1 ) {
            snprintf( k_error, sizeof(k_error), "Ketama: ketama_add_server failed to make a valid key from %s.\n", cont->data->cont_filename );
            syslog1( LOG_INFO, k_error );
            return 0;
        }

        // attempt to load the continuum
        if ( !add_server_to_continuum( addr, newmemory, cont->data, key, slist, numservers, memory, fmodtime ) ) {
            //snprintf( k_error, sizeof(k_error), "Ketama: ketama_add_server failed to add the server to the continuum.\n" );
            syslog1( LOG_INFO, k_error );
            return 0;
        }
    } else {
        snprintf( k_error, sizeof(k_error), "Ketama: %s is already in the server list and was not added.\n", addr);
        syslog1( LOG_INFO, k_error );
    }

    return ketama_roller( &cont, cont->data->cont_filename, 1);
}

int
ketama_remove_server( char* addr, ketama_continuum cont)
{
    key_t key;
    int numservers = 0;
    unsigned long memory;
    serverinfo* slist;
    time_t fmodtime;

    // verify that a valid resource was passed in before getting its key
    if (cont == NULL) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_remove_server passed illegal pointer to a continuum resource.\n" );
        syslog1( LOG_INFO, k_error );
        return 0;
    }
    key = cont->key;

    // try to find an existsing resource with a shared memory segment in the array of tracked resources, or create a new attachment resource
    if ( !get_shm_resource(key, cont) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_remove_server failed to get a valid resource.\n" );
        syslog1( LOG_INFO, k_error  );
        return 0;
    }

    // get the new server list
    if (remove_serverinfo( addr, cont->data, &numservers, &memory, &slist)) {
        // get the shared memory segment key, validating the key that was passed in
        key = get_key( cont->data->cont_filename, &fmodtime );
        if ( key == -1 ) {
            snprintf( k_error, sizeof(k_error), "Ketama: ketama_remove_server failed to make a valid key from %s.\n", cont->data->cont_filename );
            syslog1( LOG_INFO, k_error );
            return 0;
        }

        // attempt to purge the continuum of points for this server
        if ( !remove_server_from_continuum( addr, cont->data, key, slist, numservers, memory, fmodtime ) ) {
            snprintf( k_error, sizeof(k_error), "Ketama: ketama_remove_server failed to remove the server from the continuum.\n" );
            syslog1( LOG_INFO, k_error );
            return 0;
        }
    } else {
        snprintf( k_error, sizeof(k_error), "Ketama: %s not found and not removed.\n", addr);
        syslog1( LOG_INFO, k_error );
    }

    return ketama_roller( &cont, cont->data->cont_filename, 1);
}


static int
ketama_roller( ketama_continuum* contptr, char* filename, int roller_flag )
{
    key_t key;
    continuum_resource resource;

    // if we have a string like "key:0x1234" that means we want to set up the infastructure
    // but leave continuum creation until we manually add servers with ketama_add_server
    char cont_filename[PATH_MAX];
    time_t fmodtime;
    snprintf(cont_filename, sizeof(cont_filename), "%s", filename);
    key = get_key(cont_filename, &fmodtime);
    if ( key == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_roll failed to make a valid key from %s.\n", filename );
        syslog1( LOG_INFO, k_error );
        return 0;
    }

    // try to find an existsing resource with a shared memory segment in the array of tracked resources, or create a new attachment resource
    if ( !get_shm_resource(key, &resource) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_roller failed to get a valid resource.\n" );
        syslog1( LOG_INFO, k_error  );
        return 0;
    }

    // if the continuum is not stale or new, end here and track the resource if we somehow missed tracking it
    if ( (resource.data->fmodtime == fmodtime) && (resource.data->cont_version != 0) ) {
        **contptr = resource;
        track_shm_resource(resource);
        syslog( LOG_INFO, "Ketama: ketama_roll() successfully found a valid shared memory segment at %p with ID: %lu and key: %lu, stored into %p.\n",
            resource.data, (long unsigned int) resource.shmid, (long unsigned int) resource.key, *contptr);
        return 1;
    }

    // detach from the shared memory segment so that we can attach again in ketama_create to initialize the continuum
    if ( ketama_shmdt(resource.data) == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_roll failed to detatch from shared memory with errno: %d.\n", errno );
        syslog1( LOG_INFO, k_error );
    }

    // if we've already attempted to run ketama_create then that subroutine failed and we want to fail out
    if ( roller_flag != 0) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed to create a valid continuum.\n" );
        syslog1( LOG_INFO, k_error );
        return 0;
    }

    // attempt to run ketama_create continuum
    if ( !ketama_create_continuum( key, filename, contptr ) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed!\n" );
        syslog1( LOG_INFO, k_error );
        return 0;
    }

    // if we reach this point then we've created the continuum and reran ketama_roll to check its validity, so just log this and exit
    syslog1( LOG_INFO, "Ketama: ketama_create_continuum successfully finished.\n" );
    return 1;
}

int
ketama_roll( ketama_continuum* contptr, char* filename )
{
    // some initialization
    if (shm_resources == NULL) {
        init_shm_resource_tracker();
        syslog1( LOG_INFO, "Ketama: Resource tracker initiated.\n" );
    }

    // initiate the resource pointer
    *contptr = (ketama_continuum) malloc( sizeof(continuum_resource) );
    syslog1( LOG_INFO, "Ketama: Shared memory resource initiated.\n" );

    return ketama_roller( contptr, filename, 0);
}



void
ketama_smoke( ketama_continuum cont )
{
    int i;
    if (shm_resources != NULL) {
        for (i = 0; i < num_resources; i++) {
            ketama_shmdt(shm_resources[i].data);
        }
        free(shm_resources);
        shm_resources = NULL;
        num_resources = 0;
        shm_resources_size = 10;
    }

    if (sem_ids != NULL) {
        free(sem_ids);
        sem_ids = NULL;
        num_sem_ids = 0;
        sem_ids_size = 10;
    }

    free(cont);

    syslog1( LOG_INFO, "Ketama: Ketama completely smoked.\n" );
}


void
ketama_print_continuum( ketama_continuum cont )
{
    int a;
    key_t key;

    // verify that a valid resource was passed in before getting its key
    if (cont == NULL) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_print_continuum passed illegal pointer to a continuum resource.\n" );
        syslog1( LOG_INFO, k_error );
        return;
    }
    key = cont->key;

    // try to find an existsing resource with a shared memory segment in the array of tracked resources, or create a new attachment resource
    if ( !get_shm_resource(key, cont) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_print_continuum failed to get a valid resource.\n" );
        syslog1( LOG_INFO, k_error  );
        return;
    }

    printf( "Numpoints in continuum: %d\n", cont->data->numpoints );

    if ( cont->data->array == 0 ) {
        printf( "Continuum empty\n" );
    } else {
        for( a = 0; a < cont->data->numpoints; a++ ) {
            printf( "%s (%u)\n", cont->data->array[a].ip, cont->data->array[a].point );
        }
    }
}


int
ketama_compare( mcs *a, mcs *b )
{
    return ( a->point < b->point ) ?  -1 : ( ( a->point > b->point ) ? 1 : 0 );
}

int
serverinfo_compare( serverinfo *a, serverinfo *b )
{
    return ( strcmp(a->addr, b->addr) );
}


char*
ketama_error()
{
    return k_error;
}
