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

  if ( argc == 1 ) {
    argv[1] = "key:1234";
  }

  ketama_continuum c;
  ketama_roll( &c, *++argv );
  

  int i, count = c->data->numservers;

  printf("# servers: %u Total Memory: %lu\n", c->data->numservers, c->data->memtotal);

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%lu) %lu\n", c->data->slist[i].addr, c->data->slist[i].memory, (unsigned long int)&c->data->slist[i] );
  }
  printf( "\n\n\n");

  count = c->data->numpoints > 10 ? 10 : c->data->numpoints;

  printf("# points: %u\n", c->data->numpoints);

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%u) %lu\n", c->data->array[i].ip, c->data->array[i].point, (unsigned long int)&c->data->array[i] );
  }
  printf( "\n\n\n");




  ketama_remove_server( "10.0.1.6:11211", c);
  ketama_remove_server( "10.0.1.81:11211", c);
  ketama_remove_server( "10.0.1.82:11211", c);

  count = c->data->numservers;

  printf("# servers: %u Total Memory: %lu\n", c->data->numservers, c->data->memtotal);

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%lu) %lu\n", c->data->slist[i].addr, c->data->slist[i].memory, (unsigned long int)&c->data->slist[i] );
  }
  printf( "\n\n\n");

  count = c->data->numpoints > 10 ? 10 : c->data->numpoints;

  printf("# points: %u\n", c->data->numpoints);

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%u) %lu\n", c->data->array[i].ip, c->data->array[i].point, (unsigned long int)&c->data->array[i] );
  }
  printf( "\n\n\n");




  ketama_add_server( "10.0.1.6:11211", 300, c);
  ketama_add_server( "10.0.1.81:11211", 300, c);
  ketama_add_server( "10.0.1.82:11211", 300, c);

  count = c->data->numservers;

  printf("# servers: %u Total Memory: %lu\n", c->data->numservers, c->data->memtotal);

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%lu) %lu\n", c->data->slist[i].addr, c->data->slist[i].memory, (unsigned long int)&c->data->slist[i] );
  }
  printf( "\n\n\n");

  count = c->data->numpoints > 10 ? 10 : c->data->numpoints;

  printf("# points: %u\n", c->data->numpoints);

  for ( i = 0; i < count; i++ )
  {
    printf( "%s (%u) %lu\n", c->data->array[i].ip, c->data->array[i].point, (unsigned long int)&c->data->array[i] );
  }
  printf( "\n\n\n");




  printf( "%s\n", ketama_error() );

  //ketama_smoke(c);
  return 0;
}
