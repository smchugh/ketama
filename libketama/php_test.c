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

#define LOWERCASE_OFFSET 97

int main(int argc, char **argv)
{

  if ( argc == 1 ) {
    argv[1] = "key:1234";
  }

  ketama_continuum c;
  ketama_roll( &c, *++argv );

  char key[2];
  int i, count;
  count = c->data->numpoints > 10 ? 10 : c->data->numpoints;

  printf("array(%d) {\n", count);
  for ( i = 0; i < count; i++ )
  {
    key[0] = (char) i % 8 + LOWERCASE_OFFSET;
    key[1] = '\0';
    mcs* m = ketama_get_server(key, c);
    printf("  [%d]=>\n  array(2) {\n    [\"point\"]=>\n    int(%u)\n    [\"ip\"]=>\n    string(14) \"%s\"\n  }\n", i, m->point, m->ip);
  }
  printf("}\n");


  ketama_add_server( "8.7.6.5:4321", 600, c);
  count = c->data->numpoints > 10 ? 10 : c->data->numpoints;

  printf("array(%d) {\n", count);
  for ( i = 0; i < count; i++ )
  {
    key[0] = (char) i % 8 + LOWERCASE_OFFSET;
    key[1] = '\0';
    mcs* m = ketama_get_server(key, c);
    printf("  [%d]=>\n  array(2) {\n    [\"point\"]=>\n    int(%u)\n    [\"ip\"]=>\n    string(14) \"%s\"\n  }\n", i, m->point, m->ip);
  }
  printf("}\n");


  printf( "%s\n", ketama_error() );

  ketama_smoke(c);
  return 0;
}
