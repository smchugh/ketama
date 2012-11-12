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
#include "md5.h"

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


char k_error[255] = "";

int num_sem_ids = 0;
int sem_ids_size = 10;
int *sem_ids = NULL;

int num_resources = 0;
int shm_resources_size = 10;
ketama_continuum *shm_resources = NULL;

static void
init_shm_resource_tracker() {
    shm_resources = (ketama_continuum*) malloc( sizeof(ketama_continuum) * shm_resources_size );
    if (shm_resources == NULL) {
        syslog( LOG_INFO, "Ketama: Cannot malloc shm resource tracker.\n" );
        exit(1);
    }
}

static void
track_shm_resource(ketama_continuum resource) {
    int i, indx = -1;

    // find the resource and update it
    for (i = 0; i < num_resources; i++) {
        if (shm_resources[i].shmid == resource.shmid) {
            // if the continuum pointer is invalid don't count this as a found resource
            if (shm_resources[i].data != NULL) {
                indx = i;
                shm_resources[i].data = resource.data;
                shm_resources[i].key = resource.key;
            }
        }
    }

    // if we didn't find it, create a new one
    if (indx == -1) {
        if (num_resources == shm_resources_size) {
            shm_resources = (ketama_continuum*) realloc( shm_resources, sizeof(ketama_continuum) * shm_resources_size * 2 );
            if (shm_resources == NULL) {
                syslog( LOG_INFO, "Ketama: Cannot realloc shm resource tracker.\n" );
                exit(1);
            }

            shm_resources_size *= 2;
        }

        shm_resources[num_resources] = resource;
        num_resources++;
    }
}

static ketama_continuum
get_shm_resource(int shmid) {
    ketama_continuum resource;
    int i, indx = -1;

    // find the resource
    for (i = 0; i < num_resources; i++) {
        if (shm_resources[i].shmid == shmid) {
            // if the continuum pointer is invalid don't count this as a found resource
            if (shm_resources[i].data != NULL) {
                indx = i;
            }
        }
    }

    // if not found, send back an initialized resource
    if (indx == -1) {
        resource.shmid = 0;
        resource.key = 0;
        resource.data = NULL;
    } else {
        resource = shm_resources[indx];
    }

    // return the found resource or NULL
    return resource;
}

static int
ketama_shmdt(void* data)
{
    int result;

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
        syslog( LOG_INFO, "Ketama: Cannot malloc semaphore tracker.\n" );
        exit(1);
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
                syslog( LOG_INFO, "Ketama: Cannot realloc semids.\n" );
                exit(1);
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
    }

    int sem_set_id;

    sem_set_id = semget( key, 1, IPC_CREAT | 0666 );

    if ( sem_set_id == -1 )
    {
        snprintf( k_error, sizeof(k_error), "Ketama: Could not open semaphore!\n" );
        syslog( LOG_INFO, k_error );
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
void
ketama_md5_digest( char* inString, unsigned char *md5pword )
{
    md5_state_t md5state;

    md5_init( &md5state );
    md5_append( &md5state, (unsigned char *)inString, strlen( inString ) );
    md5_finish( &md5state, md5pword );
}


/** \brief Retrieve the modification time of a file.
  * \param filename The full path to the file.
  * \return The timestamp of the latest modification to the file. */
static time_t
file_modtime( char* filename )
{
    (void) filename;
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
    if (!strncmp(filename, "key:", 4)) {
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

    char* mem = NULL;
    char* endptr = NULL;

    snprintf( server->addr, sizeof(server->addr), "%s", tok );

    tok = strtok_r( NULL, delim, &saveptr );

    /* We do not check for a NULL return earlier because strtok will
     * always return at least the first token; hence never return NULL.
     */
    if ( tok == 0 )
    {
        snprintf( k_error, sizeof(k_error), "Ketama: Unable to find delimiter.\n" );
        syslog( LOG_INFO, k_error );
        server->memory = 0;
    }
    else
    {
        mem = strdup( tok );

        errno = 0;
        server->memory = strtol( mem, &endptr, 10 );
        if ( errno == ERANGE || endptr == mem )
        {
            snprintf( k_error, sizeof(k_error), "Ketama: Invalid memory value.\n" );
            syslog( LOG_INFO, k_error );
            server->memory = 0;
        }

        free( mem );
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
            syslog( LOG_INFO, k_error );
            if (fi != NULL) {
                fclose( fi );
            }
            return 0;
        }
    }

    if ( !fi ) {
        snprintf( k_error, sizeof(k_error), "Ketama: File %s doesn't exist!", filename );
        syslog( LOG_INFO, k_error );

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
  * \return The temporary address of the list of servers with the new server added in */
serverinfo*
add_serverinfo( char* addr, unsigned long newmemory, continuum* cont, int* count, unsigned long* memory)
{
    int i, numservers;
    unsigned long memtotal;
    serverinfo newserver, *newslist = 0;
    newserver.memory = 0;

    // get the current number of servers and available total memory
    numservers = cont->numservers;
    memtotal = cont->memtotal;

    // populate the new server struct
    snprintf( newserver.addr, sizeof(newserver.addr), "%s", addr );
    newserver.memory = newmemory;

    // add the server to the list
    newslist = (serverinfo*) malloc( sizeof( serverinfo ) * ( numservers + 1 ) );
    for (i = 0; i < numservers; i++) {
        memcpy( &newslist[i], &cont->slist[i], sizeof(serverinfo) );
    }
    memcpy( &newslist[numservers], &newserver, sizeof(serverinfo) );
    numservers++;
    memtotal += newmemory;

    // sort the server list
    qsort( (void *) newslist, numservers, sizeof( serverinfo ), (compfn)serverinfo_compare );

    *count = numservers;
    *memory = memtotal;

    return newslist;
}

/** \brief Removes a server from the continuum struct
  * \param addr The address of the server that you want to remove.
  * \param cont Pointer to the continuum which we will refresh. 
  * \param count The value of this pointer will be set to the amount of servers which could be parsed.
  * \param memory The value of this pointer will be set to the total amount of allocated memory across all servers.
  * \return The temporary address of the list of servers with the new server added in */
serverinfo*
remove_serverinfo( char* addr, continuum* cont, int* count, unsigned long* memory)
{
    int i, j, numservers, rm_indx = -1;
    unsigned long memtotal, oldmemory = 0;
    serverinfo *newslist;

    // get the current number of servers and available total memory
    numservers = cont->numservers;
    memtotal = cont->memtotal;
    
    for (i = 0; i < numservers; i++) {
        if (!strcmp(addr, cont->slist[i].addr)) {
            rm_indx = i;
        }
    }
    // if we didn't find the server, don't remove anything and throw a warning
    if (rm_indx == -1) {
        // create a local copy of the slist in shared memory to pass into load_continuum, which will free it
        newslist = (serverinfo*) malloc( sizeof( serverinfo ) * ( numservers ) );
        for (i = 0; i < numservers; i++) {
            memcpy( &newslist[i], &cont->slist[i], sizeof(serverinfo) );
        }
        snprintf( k_error, sizeof(k_error), "Ketama: %s not found and not removed!", addr);
        syslog( LOG_INFO, k_error );
    } else {
        newslist = (serverinfo*) malloc( sizeof( serverinfo ) * ( numservers - 1 ) );
        for (i = 0, j = 0; i < numservers; i++) {
            if (i != rm_indx) {
                memcpy( &newslist[j], &cont->slist[i], sizeof(serverinfo) );
                j++;
            } else {
                oldmemory = cont->slist[i].memory;
            }
        }
        numservers--;
        memtotal -= oldmemory;

        // sort the server list
        qsort( (void *) newslist, numservers, sizeof( serverinfo ), (compfn)serverinfo_compare );
        snprintf( k_error, sizeof(k_error), "Ketama: %s removed!", addr);
        syslog( LOG_INFO, k_error );
    }

    *count = numservers;
    *memory = memtotal;

    return newslist;
}


unsigned int
ketama_hashi( char* inString )
{
    unsigned char digest[16];

    ketama_md5_digest( inString, digest );
    return (unsigned int)(( digest[3] << 24 )
                        | ( digest[2] << 16 )
                        | ( digest[1] <<  8 )
                        |   digest[0] );
}


mcs*
ketama_get_server( char* key, ketama_continuum resource )
{
    unsigned int h = ketama_hashi( key );
    continuum* cont = resource.data;
    int highp = cont->numpoints;
    int lowp = 0, midp;
    unsigned int midval, midval1;

    // divide and conquer array search to find server with next biggest
    // point after what this key hashes to
    while ( 1 ) {
        midp = (int)( ( lowp+highp ) / 2 );

        if ( midp == cont->numpoints )
            return &cont->array[0]; // if at the end, roll back to zeroth

        midval = cont->array[midp].point;
        midval1 = midp == 0 ? 0 : cont->array[midp-1].point;

        if ( h <= midval && h > midval1 )
            return &cont->array[midp];

        if ( midval < h )
            lowp = midp + 1;
        else
            highp = midp - 1;

        if ( lowp > highp )
            return &cont->array[0];
    }
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
    int shmid, sem_set_id;
    int maxpoints = 160 * numservers;
    continuum* data;  // Pointer to shmem location

    // Continuum will hold one mcs for each point on the circle:
    mcs *ring = (mcs*) malloc( maxpoints * sizeof(mcs) );
    int i, k, numpoints = 0;

    // buildup the continuum ring
    for( i = 0; i < numservers; i++ )
    {
        float pct = (float) slist[i].memory / (float)memory;
        int ks = floorf( pct * 40.0 * (float)numservers );
#ifdef DEBUG
        int hpct = floorf( pct * 100.0 );
        syslog( LOG_INFO, "Ketama: Server no. %d: %s (mem: %lu = %u%% or %d of %d)\n",
            i, slist[i].addr, slist[i].memory, hpct, ks, numservers * 40 );
#endif

        for( k = 0; k < ks; k++ )
        {
            // 40 hashes, 4 numbers per hash = 160 points per server
            char ss[30];
            unsigned char digest[16];

            snprintf( ss, sizeof(ss), "%s-%d", slist[i].addr, k );
            ketama_md5_digest( ss, digest );

            // Use successive 4-bytes from hash as numbers for the points on the circle:
            int h;
            for( h = 0; h < 4; h++ )
            {
                ring[numpoints].point = ( digest[3+h*4] << 24 )
                                      | ( digest[2+h*4] << 16 )
                                      | ( digest[1+h*4] <<  8 )
                                      |   digest[h*4];

                snprintf( ring[numpoints].ip, sizeof(ring[numpoints].ip), "%s", slist[i].addr);
                numpoints++;
                if (numpoints > maxpoints) {
                    snprintf( k_error, sizeof(k_error), "Ketama: load_continuum tried to exceed mcs array bounds.\n" );
                    syslog( LOG_INFO, k_error );
                    free(ring);
                    free(slist);
                    return 0;
                }
            }
        }
    }

    // sort the ring in ascending order of "point"
    qsort( (void*) ring, numpoints, sizeof( mcs ), (compfn)ketama_compare );

    // attempt to obtain the shared memory ID assigned to this key, and create a segment if it doesn't exist
    shmid = shmget( key, MC_SHMSIZE, 0644 | IPC_CREAT );
    if ( shmid == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: load_continuum failed to get valid shared memory segment with errno: %d.\n", errno );
        syslog( LOG_INFO, k_error );
        return 0;
    }

    // lock the semaphore to prevent other writing to the shared memory segment
    sem_set_id = ketama_sem_init( key );
    ketama_sem_safely_lock( sem_set_id );
    int invalid_write = 0;

    // create an attachment in virtual memory to the shared memory segment, and failout if that an error is returned
    data = (continuum*) shmat( shmid, (void *)0, 0 );
    if ( data == (void *)(-1) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: load_continuum failed to attach shared memory segment with errno: %d.\n", errno );
        invalid_write = 1;
    }

    // verify that the mcs array can hold this many points
    if ( sizeof(data->array) < sizeof( mcs ) * numpoints ) {
        snprintf( k_error, sizeof(k_error), "Ketama: load_continuum tried to exceed size of mcs array.\n" );
        ketama_shmdt(data);
        invalid_write = 1;
    }

    // verify that the serverinfo array can hold this many servers
    if ( sizeof(data->slist) < sizeof( serverinfo) * numservers ) {
        snprintf( k_error, sizeof(k_error), "Ketama: load_continuum tried to exceed size of servers array.\n" );
        ketama_shmdt(data);
        invalid_write = 1;
    }

    // gracefully failout if any of the preceeding checks discovered errors
    if (invalid_write) {
        syslog( LOG_INFO, k_error );
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

    data->cont_version++;
    data->cont_modtime = time(NULL);

    // We detatch here because we will re-attach in read-only mode to actually use it.
    if (ketama_shmdt(data) == -1) {
        snprintf( k_error, sizeof(k_error), "Ketama: load_continuum failed to detatch from shared memory with errno: %d.\n", errno );
        syslog( LOG_INFO, k_error );
    }
    // unlock the semaphore
    ketama_sem_unlock( sem_set_id );

    // clean up
    free(ring);
    free(slist);

    return 1;
}

/** \brief Rolls the ketama, or checks for propper creation based on passed in flag
  * \param resource_ptr The value of this pointer will contain the retrieved continuum.
  * \param filename The server-definition file which defines our continuum.
  * \param roller_flag 0 to allow creation if continuum is stale or non-existent, 
  *                    1 to only allow a check for a valid continuum
  * \return 0 on failure, 1 on success. */
static int ketama_roller( ketama_continuum* resource_ptr, char* filename, int roller_flag );


/** \brief Generates the continuum of servers (each server as many points on a circle).
  * \param key Shared memory key for storing the newly created continuum.
  * \param filename Server definition file, which will be parsed to create this continuum.
  * \param resource_ptr The value of this pointer will contain the retrieved continuum resource.
  * \return 0 on failure, 1 on success. */
static int
ketama_create_continuum(key_t key, char* filename, ketama_continuum* resource_ptr)
{
    int shmid, sem_set_id;
    int numservers = 0;
    unsigned long memory;
    serverinfo* slist;

    // attempt to obtain the shared memory ID assigned to this key, and create a segment if it doesn't exist
    shmid = shmget( key, MC_SHMSIZE, 0644 | IPC_CREAT );
    if ( shmid == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed to get valid shared memory segment with errno: %d.\n", errno );
        syslog( LOG_INFO, k_error );
        return 0;
    }

    // if filename is not a user-specific key, init a shared memory segment from the pathname, if it's valid
    if (strncmp(filename, "key:", 4)) {
        // get the list of servers from the provided file
        slist = read_server_definitions( filename, &numservers, &memory );

        // Check numservers first; if it is zero then there is no error message and we need to set one.
        if ( numservers < 1 ) {
            snprintf( k_error, sizeof(k_error), "Ketama: No valid server definitions in file %s", filename );
            syslog( LOG_INFO, k_error );
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
            syslog( LOG_INFO, k_error );
            return 0;
        }
    }

    // lock the semaphore to prevent other writing to the shared memory segment
    sem_set_id = ketama_sem_init( key );
    ketama_sem_safely_lock( sem_set_id );

    // create an attachment in virtual memory to the shared memory segment, and failout if that an error is returned
    continuum* data = (continuum*) shmat( shmid, (void *)0, 0 );
    if ( data == (void *)(-1) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed to attach shared memory segment with errno: %d.\n", errno );
        syslog( LOG_INFO, k_error  );
        ketama_sem_unlock( sem_set_id );
        return 0;
    }

    // if filename is not a path, init an empty shared memory segment, otherwise just record the filename
    if (!strncmp(filename, "key:", 4)) {
        data->fmodtime = 0;
        data->cont_version = 1;
        data->cont_modtime = time(NULL);
    }
    snprintf(data->cont_filename, sizeof(data->cont_filename), "%s", filename);

    // attempt to detach the shared memory segment and throw an error if one is received
    if (ketama_shmdt(data) == -1) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed to detatch from shared memory with errno: %d.\n", errno );
        syslog( LOG_INFO, k_error );
    }

    // unlock the semaphore
    ketama_sem_unlock( sem_set_id );

    // rerun ketama_roller with the roller flag on to validate the loaded continuum
    return ketama_roller( resource_ptr, filename, 1);
}

int
ketama_add_server( char* addr, unsigned long newmemory, ketama_continuum resource)
{
    key_t key;
    continuum* cont = resource.data;
    int numservers = 0;
    unsigned long memory;
    serverinfo* slist;
    time_t fmodtime;

    // get the new server list
    slist = add_serverinfo( addr, newmemory, cont, &numservers, &memory);

    // get the shared memory segment key
    key = get_key( cont->cont_filename, &fmodtime );
    if ( key == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_add_server failed to make a valid key from %s.\n", cont->cont_filename );
        syslog( LOG_INFO, k_error );
        return 0;
    }

    // attempt to load the continuum
    if ( !load_continuum( key, slist, numservers, memory, fmodtime ) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_add_server failed to load the continuum.\n" );
        syslog( LOG_INFO, k_error );
        return 0;
    }

    return ketama_roller( &resource, cont->cont_filename, 1);
}

int
ketama_remove_server( char* addr, ketama_continuum resource)
{
    key_t key;
    continuum* cont = resource.data;
    int numservers = 0;
    unsigned long memory;
    serverinfo* slist;
    time_t fmodtime;

    // get the new server list
    slist = remove_serverinfo( addr, cont, &numservers, &memory);
    
    // get the shared memory segment key
    key = get_key( cont->cont_filename, &fmodtime );
    if ( key == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_remove_server failed to make a valid key from %s.\n", cont->cont_filename );
        syslog( LOG_INFO, k_error );
        return 0;
    }

    // attempt to load the continuum
    if ( !load_continuum( key, slist, numservers, memory, fmodtime ) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_remove_server failed to load the continuum.\n" );
        syslog( LOG_INFO, k_error );
        return 0;
    }
    
    return ketama_roller( &resource, cont->cont_filename, 1);
}


static int
ketama_roller( ketama_continuum* resource_ptr, char* filename, int roller_flag )
{
    key_t key;
    int shmid;
    ketama_continuum resource;

    // if we have a string like "key:0x1234" that means we want to set up the infastructure 
    // but leave continuum creation until we manually add servers with ketama_add_server
    char cont_filename[PATH_MAX];
    time_t fmodtime;
    snprintf(cont_filename, sizeof(cont_filename), "%s", filename);
    key = get_key(cont_filename, &fmodtime);
    if ( key == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_roll failed to make a valid key from %s.\n", filename );
        syslog( LOG_INFO, k_error );
        return 0;
    }

    // attempt to obtain the shared memory ID assigned to this key, and create a segment if it doesn't exist
    shmid = shmget( key, MC_SHMSIZE, 0644 | IPC_CREAT );
    if ( shmid == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_roll failed to get valid shared memory segment with errno: %d.\n", errno );
        syslog( LOG_INFO, k_error );
        return 0;
    }

    // try to find the shared memory segment in the array of tracked resources, or create a new attachment in virtual memory
    resource = get_shm_resource(shmid);
    if ( resource.shmid == 0 ) {
        resource.data = (continuum*) shmat( shmid, (void *)0, SHM_RDONLY );
        if ( resource.data == (void *)(-1) ) {
            snprintf( k_error, sizeof(k_error), "Ketama: ketama_roll failed to attach shared memory segment with errno: %d.\n", errno );
            syslog( LOG_INFO, k_error  );
            return 0;
        }
        resource.shmid = shmid;
        resource.key = key;
    }

    // if the continuum is not stale or new, end here and track the resource if we somehow missed tracking it
    if ( (resource.data->fmodtime == fmodtime) && (resource.data->cont_version != 0) ) {
        *resource_ptr = resource;
        track_shm_resource(resource);
        syslog( LOG_INFO, "Ketama: ketama_roll() successfully found a valid shared memory segment.\n" );
        return 1;
    }

    // detach from the shared memory segment so that we can attach again in ketama_create to initialize the continuum
    if ( ketama_shmdt(resource.data) == -1 ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_roll failed to detatch from shared memory with errno: %d.\n", errno );
        syslog( LOG_INFO, k_error );
    }

    // if we've already attempted to run ketama_create then that subroutine failed and we want to fail out
    if ( roller_flag ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed to create a valid continuum.\n" );
        syslog( LOG_INFO, k_error );
        return 0;
    }

    // attempt to run ketama_create continuum
    if ( !ketama_create_continuum( key, filename, resource_ptr ) ) {
        snprintf( k_error, sizeof(k_error), "Ketama: ketama_create_continuum failed!\n" );
        syslog( LOG_INFO, k_error );
        return 0;
    }

    // if we reach this point then we've created the continuum and reran ketama_roll to check its validity, so just log this and exit
    syslog( LOG_INFO, "Ketama: ketama_create_continuum successfully finished.\n" );
    return 1;
}

int
ketama_roll( ketama_continuum* resource_ptr, char* filename )
{
    // some initialization
    if (shm_resources == NULL) {
        init_shm_resource_tracker();
    }

    return ketama_roller( resource_ptr, filename, 0);
}



void
ketama_smoke( ketama_continuum resource )
{
    int i;
    if (shm_resources != NULL) {
        for (i = 0; i < num_resources; i++) {
            ketama_shmdt(shm_resources[i].data);
            shmctl(shm_resources[i].shmid, IPC_RMID, 0);
        }
        free(shm_resources);
        shm_resources = NULL;
        num_resources = 0;
        shm_resources_size = 10;
    }

    if (sem_ids != NULL) {
        for (i = 0; i < num_sem_ids; i++) {
            semctl(sem_ids[i], 0, IPC_RMID, 0);
        }
        free(sem_ids);
        sem_ids = NULL;
        num_sem_ids = 0;
        sem_ids_size = 10;
    }

    (void) resource;

    syslog( LOG_INFO, "Ketama: Ketama completely smoked.\n" );
}


void
ketama_print_continuum( ketama_continuum resource )
{
    int a;
    continuum* cont = resource.data;

    printf( "Numpoints in continuum: %d\n", cont->numpoints );

    if ( cont->array == 0 ) {
        printf( "Continuum empty\n" );
    } else {
        for( a = 0; a < cont->numpoints; a++ ) {
            printf( "%s (%u)\n", cont->array[a].ip, cont->array[a].point );
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
