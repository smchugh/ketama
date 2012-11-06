/*
 * Using a known ketama.servers file, and a fixed set of keys
 * print and hash the output of this program using your modified
 * libketama, compare the hash of the output to the known correct
 * hash in the test harness.
 *
 */

#include "ketama.h"
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv)
{

  if( argc == 1 ){
	  //printf("Usage: %s <ketama.servers file>\n", *argv);
	  //return 1;
    argv[1] = "";
  }

  ketama_continuum c;
  ketama_roll( &c, *++argv );

  int i, count = c->numservers;

  printf("# servers: %u Total Memory: %lu\n", c->numservers, c->memtotal);

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%lu) %lu\n", c->slist[i].addr, c->slist[i].memory, (unsigned long int)&c->slist[i] );
  }
  printf( "\n\n\n");

  count = c->numpoints > 10 ? 10 : c->numpoints;

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%u) %lu\n", c->array[i].ip, c->array[i].point, (unsigned long int)&c->array[i] );
  }
  printf( "\n\n\n");

  


  ketama_remove_server( "10.0.1.2:11211", c);
  ketama_remove_server( "10.0.1.81:11211", c);
  ketama_remove_server( "10.0.1.82:11211", c);

  count = c->numservers;

  printf("# servers: %u Total Memory: %lu\n", c->numservers, c->memtotal);

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%lu) %lu\n", c->slist[i].addr, c->slist[i].memory, (unsigned long int)&c->slist[i] );
  }
  printf( "\n\n\n");

  count = c->numpoints > 10 ? 10 : c->numpoints;

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%u) %lu\n", c->array[i].ip, c->array[i].point, (unsigned long int)&c->array[i] );
  }
  printf( "\n\n\n");




  ketama_add_server( "10.0.1.2:11211", 300, c);
  ketama_add_server( "10.0.1.81:11211", 300, c);
  ketama_add_server( "10.0.1.82:11211", 300, c);

  count = c->numservers;

  printf("# servers: %u Total Memory: %lu\n", c->numservers, c->memtotal);

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%lu) %lu\n", c->slist[i].addr, c->slist[i].memory, (unsigned long int)&c->slist[i] );
  }
  printf( "\n\n\n");

  count = c->numpoints > 10 ? 10 : c->numpoints;

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%u) %lu\n", c->array[i].ip, c->array[i].point, (unsigned long int)&c->array[i] );
  }
  printf( "\n\n\n");




  printf( "%s\n", ketama_error() );

  ketama_smoke(c);
  return 0;
}
