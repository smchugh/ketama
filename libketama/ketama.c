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
int num_shm_ids = 0;
int num_data = 0;
int sem_ids_size = 1024;
int shm_ids_size = 1024;
int shm_data_size = 1024;
int *sem_ids = NULL;
int *shm_ids = NULL;
int **shm_data = NULL;

static void
init_sem_id_tracker() {
    sem_ids = malloc(sizeof(int)*sem_ids_size);
}

static void
init_shm_id_tracker() {
    shm_ids = malloc(sizeof(int)*shm_ids_size);
}

static void
init_shm_data_tracker() {
    shm_data = malloc(sizeof(int*)*shm_data_size);
}

static void
track_shm_data(int *data) {
    if (num_data == shm_data_size) {
        void *tmp = realloc(shm_data, sizeof(int*) * shm_data_size * 2);
        if (tmp != NULL) {
            shm_data = tmp;
        } else {
            snprintf( k_error, sizeof(k_error), "%s", "Cannot realloc shm data tracker" );
            exit(1);
        }

        shm_data_size *= 2;
    }

    shm_data[num_data] = data;
    num_data++;
}

static void
track_sem_id(int semid) {
    if (num_sem_ids == sem_ids_size) {
    void *tmp = realloc(sem_ids, sizeof(int) * sem_ids_size * 2);
        if (tmp != NULL) {
            sem_ids = tmp;
        } else {
            snprintf( k_error, sizeof(k_error), "%s", "Cannot realloc semids" );
            exit(1);
        }

        sem_ids_size *= 2;
    }

    sem_ids[num_sem_ids] = semid;
    num_sem_ids++;
}

static void
track_shm_id(int shmid) {
    if (num_shm_ids == shm_ids_size) {
        void *tmp = realloc(shm_ids, sizeof(int) * shm_ids_size * 2);
        if (tmp != NULL) {
            shm_ids = tmp;
        } else {
            snprintf( k_error, sizeof(k_error), "%s", "Cannot realloc shmids" );
            exit(1);
        }

        shm_ids_size *= 2;
    }

    shm_ids[num_shm_ids] = shmid;
    num_shm_ids++;
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

    sem_set_id = semget( key, 1, 0 );

    if ( sem_set_id == -1 )
    {
        // create a semaphore set with ID SEM_ID
        sem_set_id = semget( key, 1, IPC_CREAT | 0666 );
        track_sem_id(sem_set_id);

        if ( sem_set_id == -1 )
        {
            snprintf( k_error, sizeof(k_error), "%s", "Could not open semaphore!" );
            return 0;
        }

        ketama_sem_unlock( sem_set_id );
    }

    // track the semaphore if it exists
    track_sem_id(sem_set_id);

    return sem_set_id;
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
        snprintf( k_error, sizeof(k_error), "%s", "Unable to find delimiter" );
        syslog( LOG_INFO, "Unable to find delimiter.\n" );
        server->memory = 0;
    }
    else
    {
        mem = strdup( tok );

        errno = 0;
        server->memory = strtol( mem, &endptr, 10 );
        if ( errno == ERANGE || endptr == mem )
        {
            snprintf( k_error, sizeof(k_error), "%s", "Invalid memory value" );
            syslog( LOG_INFO, "Invalid memory value.\n" );
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
        if ( server.memory > 0 && strlen( server.addr ) > 0 )
        {
            slist = (serverinfo*)realloc( slist, sizeof( serverinfo ) * ( numservers + 1 ) );
            memcpy( &slist[numservers], &server, sizeof( serverinfo ) );
            numservers++;
            memtotal += server.memory;
        }
        else
        {
            /* This kind of tells the parent code that
             * "there were servers but not really"
             */
            *count = 1;
            free( slist );
            snprintf( k_error, sizeof(k_error), "%s (line %d in %s)", k_error, lineno, filename );
            syslog( LOG_INFO, "Server %s with memory %lu could not be loaded.\n", server.addr, server.memory );
            if (fi != NULL) {
                fclose( fi );
            }
            return 0;
        }
    }

    if ( !fi )
    {
        snprintf( k_error, sizeof(k_error), "File %s doesn't exist!", filename );
        syslog( LOG_INFO, "File %s doesn't exist!\n", filename );

        *count = 0;
        return 0;
    }
    else
    {
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
add_serverinfo( char* addr, unsigned long newmemory, ketama_continuum cont, int* count, unsigned long* memory)
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
    for (i = 0; i < numservers; i++)
    {
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
remove_serverinfo( char* addr, ketama_continuum cont, int* count, unsigned long* memory)
{
    int i, j, numservers, rm_indx = -1;
    unsigned long memtotal, oldmemory = 0;
    serverinfo *newslist;

    // get the current number of servers and available total memory
    numservers = cont->numservers;
    memtotal = cont->memtotal;
    
    for (i = 0; i < numservers; i++)
    {
        if (!strcmp(addr, cont->slist[i].addr)) {
            rm_indx = i;
        }
    }
    // if we didn't find the server, don't remove anything and throw a warning
    if (rm_indx == -1) {
        newslist = NULL;
        snprintf( k_error, sizeof(k_error), "%s not found and not removed!", addr);
        syslog( LOG_INFO, "%s not found and not removed!\n", addr );
    } else {
        newslist = (serverinfo*) malloc( sizeof( serverinfo ) * ( numservers - 1 ) );
        for (i = 0, j = 0; i < numservers; i++)
        {
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
ketama_get_server( char* key, ketama_continuum cont )
{
    unsigned int h = ketama_hashi( key );
    int highp = cont->numpoints;
    int lowp = 0, midp;
    unsigned int midval, midval1;

    // divide and conquer array search to find server with next biggest
    // point after what this key hashes to
    while ( 1 )
    {
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
  * \param modtime File modtime
  * \return 0 on failure, 1 on success. */
int
load_continuum(key_t key, serverinfo* slist, int numservers, unsigned long memory, time_t modtime)
{
    int shmid, maxpoints = 160 * numservers;
    ketama_continuum data;  /* Pointer to shmem location */

    /* Continuum will hold one mcs for each point on the circle: */
    mcs *ring = (mcs*) malloc( maxpoints * sizeof(mcs) );
    serverinfo *servers = (serverinfo*) malloc( numservers * sizeof(serverinfo) );
    int i, k, numpoints = 0;

    for( i = 0; i < numservers; i++ )
    {
        float pct = (float) slist[i].memory / (float)memory;
        int ks = floorf( pct * 40.0 * (float)numservers );
        int hpct = floorf( pct * 100.0 );

        syslog( LOG_INFO, "Server no. %d: %s (mem: %lu = %u%% or %d of %d)\n",
            i, slist[i].addr, slist[i].memory, hpct, ks, numservers * 40 );

        // copy over the slist to usable memory
        memcpy( &servers[i], &slist[i], sizeof(serverinfo) );

        for( k = 0; k < ks; k++ )
        {
            /* 40 hashes, 4 numbers per hash = 160 points per server */
            char ss[30];
            unsigned char digest[16];

            snprintf( ss, sizeof(ss), "%s-%d", slist[i].addr, k );
            ketama_md5_digest( ss, digest );

            /* Use successive 4-bytes from hash as numbers
             * for the points on the circle: */
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
                    snprintf( k_error, sizeof(k_error), "%s", "Tried to exceed mcs array bounds.\n" );
                    syslog( LOG_INFO, "Error: Tried to exceed mcs array bounds.\n" );
                    free( slist );
                    return 0;
                }
            }
        }
    }
    free( slist );

    /* Sorts in ascending order of "point" */
    qsort( (void*) ring, numpoints, sizeof( mcs ), (compfn)ketama_compare );

    /* Add data to shmmem */
    shmid = shmget( key, MC_SHMSIZE, 0644 | IPC_CREAT );
    track_shm_id(shmid);

    data = (ketama_continuum) shmat( shmid, (void *)0, 0 );
    if ( data == (void *)(-1) )
    {
        snprintf( k_error, sizeof(k_error), "%s", "Can't open shmmem for writing." );
        syslog( LOG_INFO, "Can't open shmmem for writing.\n" );
        return 0;
    }

    if ( sizeof(data->array) < sizeof( mcs ) * numpoints ) {
        snprintf( k_error, sizeof(k_error), "%s", "Tried to exceed size of mcs array." );
        syslog( LOG_INFO, "Tried to exceed size of mcs array.\n" );
        free(ring);
        free(servers);
        return 0;
    }

    if ( sizeof(data->slist) < sizeof( serverinfo) * numservers ) {
        snprintf( k_error, sizeof(k_error), "%s", "Tried to exceed size of servers array." );
        syslog( LOG_INFO, "Tried to exceed size of servers array.\n" );
        free(ring);
        free(servers);
        return 0;
    }

    data->numpoints = numpoints;
    data->numservers = numservers;
    data->memtotal = memory;
    data->modtime = modtime;
    memcpy( data->array, ring, sizeof( mcs ) * numpoints );
    memcpy( data->slist, servers, sizeof( serverinfo ) * numservers );

    data->cont_version++;
    data->cont_modtime = time(NULL);

    free(ring);
    free(servers);


    /* We detatch here because we will re-attach in read-only
     * mode to actually use it. */
#ifdef SOLARIS
        if ( shmdt( (char *) data ) == -1 ) {
#else
        if ( shmdt( (void *) data ) == -1 ) {
#endif
            snprintf( k_error, sizeof(k_error), "%s", "Error detatching from shared memory!" );
            syslog( LOG_INFO, "Error detatching from shared memory!\n" );
        }

    return 1;
}


/** \brief Generates the continuum of servers (each server as many points on a circle).
  * \param key Shared memory key for storing the newly created continuum.
  * \param filename Server definition file, which will be parsed to create this continuum.
  * \return 0 on failure, 1 on success. */
static int
ketama_create_continuum( key_t key, char* filename )
{
    if (shm_ids == NULL) {
        init_shm_id_tracker();
    }

    if (shm_data == NULL) {
        init_shm_data_tracker();
    }

    
    int numservers = 0;
    unsigned long memory;
    serverinfo* slist;

    slist = read_server_definitions( filename, &numservers, &memory );

    /* Check numservers first; if it is zero then there is no error message
     * and we need to set one. */
    if ( numservers < 1 )
    {
        snprintf( k_error, sizeof(k_error), "No valid server definitions in file %s", filename );
        syslog( LOG_INFO, "No valid server definitions in file %s.\n", filename );
        return 0;
    }
    else if ( slist == 0 )
    {
        /* read_server_definitions must've set error message. */
        return 0;
    }
    syslog( LOG_INFO, "Server definitions read: %u servers, total memory: %lu.\n", numservers, memory );

    time_t modtime = file_modtime( filename );

    if ( !load_continuum( key, slist, numservers, memory, modtime ) )
    {
        snprintf( k_error, sizeof(k_error), "%s", "Failed to load the continuum" );
        syslog( LOG_INFO, "Failed to load the continuum\n" );
        return 0;
    }
    
    return 1;
}

void
ketama_add_server( char* addr, unsigned long newmemory, ketama_continuum cont)
{
    key_t key;
    int shmid;
    int *data;
    int numservers = 0;
    unsigned long memory;
    serverinfo* slist;

    if (shm_ids == NULL) {
        init_shm_id_tracker();
    }

    if (shm_data == NULL) {
        init_shm_data_tracker();
    }

    // get the new server list
    slist = add_serverinfo( addr, newmemory, cont, &numservers, &memory);

    key = ftok( cont->cont_filename, 'R' );
    if ( key == -1 )
    {
        key = ketama_hashi( cont->cont_filename );
    }

    // load the continuum
    if ( !load_continuum( key, slist, numservers, memory, 0 ) )
    {
        snprintf( k_error, sizeof(k_error), "%s", "Failed to load the continuum" );
        syslog( LOG_INFO, "Failed to load the continuum\n" );
        return;
    }
    
    shmid = shmget( key, MC_SHMSIZE, 0 ); // read only attempt.
    track_shm_id(shmid);

    data = shmat( shmid, (void *)0, SHM_RDONLY );

    cont = (ketama_continuum) data;

    track_shm_data(data);
}

void
ketama_remove_server( char* addr, ketama_continuum cont)
{
    key_t key;
    int shmid;
    int *data;
    int numservers = 0;
    unsigned long memory;
    serverinfo* slist;

    if (shm_ids == NULL) {
        init_shm_id_tracker();
    }

    if (shm_data == NULL) {
        init_shm_data_tracker();
    }

    // get the new server list
    slist = remove_serverinfo( addr, cont, &numservers, &memory);
    
    key = ftok( cont->cont_filename, 'R' );
    if ( key == -1 )
    {
        key = ketama_hashi( cont->cont_filename );
    }

    // load the continuum
    if (slist != NULL) {
        if ( !load_continuum( key, slist, numservers, memory, 0 ) )
        {
            snprintf( k_error, sizeof(k_error), "%s", "Failed to load the continuum" );
            syslog( LOG_INFO, "Failed to load the continuum\n" );
            return;
        }
    }
    
    shmid = shmget( key, MC_SHMSIZE, 0 ); // read only attempt.
    track_shm_id(shmid);

    data = shmat( shmid, (void *)0, SHM_RDONLY );

    cont = (ketama_continuum) data;

    track_shm_data(data);
}


int
ketama_roll( ketama_continuum* contptr, char* filename )
{
    if (shm_ids == NULL) {
        init_shm_id_tracker();
    }

    if (shm_data == NULL) {
        init_shm_data_tracker();
    }   

    snprintf(k_error, sizeof(k_error), "%s", "");

    key_t key;
    int shmid;
    ketama_continuum data;
    int sem_set_id;

    // if we have an invalid file name that means we want to set up the infastructure 
    // but leave continuum creation until we manually add servers with ketama_add_server
    char cont_filename[PATH_MAX];
    snprintf(cont_filename, sizeof(cont_filename), "%s", filename);
    key = ftok( cont_filename, 'R' );
    if ( key == -1 )
    {
        key = ketama_hashi( filename );

        // Add empty data to shmmem
        shmid = shmget( key, MC_SHMSIZE, 0644 | IPC_CREAT );
        track_shm_id(shmid);

        data = (ketama_continuum) shmat( shmid, (void *)0, SHM_RDONLY );
        if ( data == (void *)(-1) )
        {
            snprintf( k_error, sizeof(k_error), "%s", "Can't open shmmem for writing." );
            syslog( LOG_INFO, "Can't open shmmem for writing.\n" );
            return 0;
        }

        data = (ketama_continuum) shmat( shmid, (void *)0, 0 );

        data->numpoints = 0;
        data->numservers = 0;
        data->memtotal = 0;
        data->modtime = 0;
        data->cont_version = 0;
        data->cont_modtime = time(NULL);
        snprintf(data->cont_filename, sizeof(data->cont_filename), "%s", filename);

        /* We detatch here because we will re-attach in read-only
         * mode to actually use it. */
#ifdef SOLARIS
        if ( shmdt( (char *) data ) == -1 ) {
#else
        if ( shmdt( (void *) data ) == -1 ) {
#endif
            snprintf( k_error, sizeof(k_error), "%s", "Error detatching from shared memory!" );
            syslog( LOG_INFO, "Error detatching from shared memory!\n" );
        }

        // we can end here
        data = (ketama_continuum) shmat( shmid, (void *)0, SHM_RDONLY );
        *contptr = data;
        track_shm_data((int*) data);

        return 1;
    }

    *contptr = NULL;

    sem_set_id = ketama_sem_init( key );

    int sanity = 0;
    while ( semctl( sem_set_id, 0, GETVAL, 0 ) == 2 )
    {
        // wait for the continuum creator to finish, but don't block others
        usleep( 5 );

        // if we are waiting for > 1 second, take drastic action:
        if(++sanity > 1000000)
        {
            usleep( rand()%50000 );
            ketama_sem_unlock( sem_set_id );
            break;
        }
    }

    time_t modtime = file_modtime( filename );

    shmid = shmget( key, MC_SHMSIZE, 0 ); // read only attempt.
    track_shm_id(shmid);

    data = (ketama_continuum) shmat( shmid, (void *)0, SHM_RDONLY );
    int create_needed = 0;

    if ( data != (void *)(-1) )
    {
        if (data->modtime != modtime) {
            create_needed = 1;
        }
    } else {
        create_needed = 1;
    }


    if (create_needed) {
        ketama_sem_lock( sem_set_id );

        if ( data == (void *)(-1) )
            syslog( LOG_INFO, "Shared memory empty, creating and populating...\n" );
        else
            syslog( LOG_INFO, "Server definitions changed, reloading...\n" );

        if ( !ketama_create_continuum( key, filename ) ) {
            snprintf( k_error, sizeof(k_error), "%s", "Ketama_create_continuum() failed!" );
            syslog( LOG_INFO, "Ketama_create_continuum() failed!\n" );
            ketama_sem_unlock( sem_set_id );
            return 0;
        } else {
            syslog( LOG_INFO, "ketama_create_continuum() successfully finished.\n" );
        }

        shmid = shmget( key, MC_SHMSIZE, 0 ); // read only attempt.
        track_shm_id(shmid);

        data = (ketama_continuum) shmat( shmid, (void *)0, SHM_RDONLY );
        ketama_sem_unlock( sem_set_id );
    }

    if ( data == (void *)(-1) )
    {
        snprintf( k_error, sizeof(k_error), "%s", "Failed miserably to get pointer to shmemdata!" );
        return 0;
    }

    data = (ketama_continuum) shmat( shmid, (void *)0, 0 );

    data->cont_version = 0;
    data->cont_modtime = time(NULL);
    snprintf(data->cont_filename, sizeof(data->cont_filename), "%s", filename);

    /* We detatch here because we will re-attach in read-only
     * mode to actually use it. */
#ifdef SOLARIS
        if ( shmdt( (char *) data ) == -1 ) {
#else
        if ( shmdt( (void *) data ) == -1 ) {
#endif
            snprintf( k_error, sizeof(k_error), "%s", "Error detatching from shared memory!" );
            syslog( LOG_INFO, "Error detatching from shared memory!\n" );
        }


    data = (ketama_continuum) shmat( shmid, (void *)0, SHM_RDONLY );
    *contptr = data;

    track_shm_data((int*) data);

    return 1;
}


void
ketama_smoke( ketama_continuum contptr )
{
    int i;
    if (shm_data != NULL) {
        for (i = 0; i < num_data; i++) {
#ifdef SOLARIS
            shmdt((char *)shm_data[i]);
#else
            shmdt(shm_data[i]);
#endif
        }
        free(shm_data);
        shm_data = NULL;
        num_data = 0;
        shm_data_size = 1024;
    }

    if (sem_ids != NULL) {
        for (i = 0; i < num_sem_ids; i++) {
            semctl(sem_ids[i], 0, IPC_RMID, 0);
        }
        free(sem_ids);
        sem_ids = NULL;
        num_sem_ids = 0;
        sem_ids_size = 1024;
    }

    if (shm_ids != NULL) {
        for (i = 0; i < num_shm_ids; i++) {
            shmctl(shm_ids[i], IPC_RMID, 0);
        }
        free(shm_ids);
        shm_ids = NULL;
        num_shm_ids = 0;
        shm_ids_size = 1024;
    }

    (void) contptr;

    syslog( LOG_INFO, "Ketama completely smoked.\n" );
}


void
ketama_print_continuum( ketama_continuum cont )
{
    int a;
    printf( "Numpoints in continuum: %d\n", cont->numpoints );

    if ( cont->array == 0 )
    {
        printf( "Continuum empty\n" );
    }
    else
    {
        mcs (*mcsarr)[cont->numpoints] = &(cont->array);
        for( a = 0; a < cont->numpoints; a++ )
        {
            printf( "%s (%u)\n", (*mcsarr)[a].ip, (*mcsarr)[a].point );
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
