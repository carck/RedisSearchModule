/*
 *    string pool based on strmap version 2.0.1
 *
 *    ANSI C hash table for strings.
 *
 *	  Version history:
 *	  1.0.0 - initial release
 *	  2.0.0 - changed function prefix from strmap to sm to ensure
 *	      ANSI C compatibility
 *	  2.0.1 - improved documentation
 *
 *    string_pool.c
 *
 *    Copyright (c) 2009, 2011, 2013 Per Ola Kristensson.
 *
 *    Per Ola Kristensson <pok21@cam.ac.uk>
 *    Inference Group, Department of Physics
 *    University of Cambridge
 *    Cavendish Laboratory
 *    JJ Thomson Avenue
 *    CB3 0HE Cambridge
 *    United Kingdom
 *
 *    strmap is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Lesser General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *    strmap is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Lesser General Public License for more details.
 *
 *    You should have received a copy of the GNU Lesser General Public License
 *    along with strmap.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <pthread.h>
#include "string_pool.h"

typedef struct Pair Pair;

typedef struct Bucket Bucket;

struct Pair {
  char *key;
};

struct Bucket {
  unsigned int count;
  Pair *pairs;
};

struct StringPool {
  unsigned int count;
  Bucket *buckets;
  pthread_mutex_t mutex;
};

static Pair *get_pair(Bucket *bucket, const char *key);
static unsigned int murMurHash(const void *key, int len);

StringPool *sm_new(unsigned int capacity) {
  StringPool *pool;

  pool = malloc(sizeof(StringPool));
  if (pool == NULL) {
    return NULL;
  }
  if (pthread_mutex_init(&pool->mutex, NULL) != 0) {
    sm_delete(pool);
    return NULL;
  }
  pool->count = capacity;
  pool->buckets = malloc(pool->count * sizeof(Bucket));
  if (pool->buckets == NULL) {
    free(pool);
    return NULL;
  }
  memset(pool->buckets, 0, pool->count * sizeof(Bucket));
  return pool;
}

void sm_delete(StringPool *pool) {
  unsigned int i, j, n, m;
  Bucket *bucket;
  Pair *pair;

  if (pool == NULL) {
    return;
  }
  n = pool->count;
  bucket = pool->buckets;
  i = 0;
  while (i < n) {
    m = bucket->count;
    pair = bucket->pairs;
    j = 0;
    while (j < m) {
      free(pair->key);
      pair++;
      j++;
    }
    free(bucket->pairs);
    bucket++;
    i++;
  }
  free(pool->buckets);
  free(pool);
}

char *sm_put(StringPool *pool, const char *string) {
  unsigned int key_len, index;
  Bucket *bucket;
  Pair *tmp_pairs, *pair;
  char *new_key = NULL;

  if (pool == NULL) {
    return NULL;
  }
  if (string == NULL) {
    return NULL;
  }
  key_len = strlen(string);
  /* Get a pointer to the bucket the key string hashes to */
  index = murMurHash(string, key_len) % pool->count;
  bucket = &(pool->buckets[index]);
  /* Check if we can handle insertion by simply replacing
   * an existing value in a key-value pair in the bucket.
   */
  if ((pair = get_pair(bucket, string)) != NULL) {
    return pair->key;
  }

  pthread_mutex_lock(&pool->mutex);
  if ((pair = get_pair(bucket, string)) != NULL) {
    pthread_mutex_unlock(&pool->mutex);
    return pair->key;
  }
  /* Allocate space for a new key */
  new_key = malloc((key_len + 1) * sizeof(char));
  if (new_key == NULL) {
    goto unlock;
  }
  /* Create a key-value pair */
  if (bucket->count == 0) {
    /* The bucket is empty, lazily allocate space for a single
     * key-value pair.
     */
    bucket->pairs = malloc(sizeof(Pair));
    if (bucket->pairs == NULL) {
      free(new_key);
      goto unlock;
    }
    bucket->count = 1;
  } else {
    /* The bucket wasn't empty but no pair existed that matches the provided
     * key, so create a new key-value pair.
     */
    tmp_pairs = realloc(bucket->pairs, (bucket->count + 1) * sizeof(Pair));
    if (tmp_pairs == NULL) {
      free(new_key);
      goto unlock;
    }
    bucket->pairs = tmp_pairs;
    bucket->count++;
  }
  /* Get the last pair in the chain for the bucket */
  pair = &(bucket->pairs[bucket->count - 1]);
  pair->key = new_key;
  /* Copy the key and its value into the key-value pair */
  strcpy(pair->key, string);
unlock:
  pthread_mutex_unlock(&pool->mutex);
  return new_key;
}

int sm_get_count(const StringPool *pool) {
  unsigned int i, j, n, m;
  unsigned int count;
  Bucket *bucket;
  Pair *pair;

  if (pool == NULL) {
    return 0;
  }
  bucket = pool->buckets;
  n = pool->count;
  i = 0;
  count = 0;
  while (i < n) {
    pair = bucket->pairs;
    m = bucket->count;
    j = 0;
    while (j < m) {
      count++;
      pair++;
      j++;
    }
    bucket++;
    i++;
  }
  return count;
}

/*
 * Returns a pair from the bucket that matches the provided key,
 * or null if no such pair exist.
 */
static Pair *get_pair(Bucket *bucket, const char *key) {
  unsigned int i, n;
  Pair *pair;

  n = bucket->count;
  if (n == 0) {
    return NULL;
  }
  pair = bucket->pairs;
  if (n == 1) return pair;
  i = 0;
  while (i < n) {
    if (pair->key != NULL) {
      if (strcmp(pair->key, key) == 0) {
        return pair;
      }
    }
    pair++;
    i++;
  }
  return NULL;
}

static unsigned int murMurHash(const void *key, int len) {
  const unsigned int m = 0x5bd1e995;
  const int r = 24;
  const int seed = 97;
  unsigned int h = seed ^ len;
  // Mix 4 bytes at a time into the hash
  const unsigned char *data = (const unsigned char *)key;
  while (len >= 4) {
    unsigned int k = *(unsigned int *)data;
    k *= m;
    k ^= k >> r;
    k *= m;
    h *= m;
    h ^= k;
    data += 4;
    len -= 4;
  }
  // Handle the last few bytes of the input array
  switch (len) {
    case 3:
      h ^= data[2] << 16;
    case 2:
      h ^= data[1] << 8;
    case 1:
      h ^= data[0];
      h *= m;
  };
  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.
  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;
  return h;
}

/*

           GNU LESSER GENERAL PUBLIC LICENSE
                       Version 3, 29 June 2007

 Copyright (C) 2007 Free Software Foundation, Inc. <http://fsf.org/>
 Everyone is permitted to copy and distribute verbatim copies
 of this license document, but changing it is not allowed.


  This version of the GNU Lesser General Public License incorporates
the terms and conditions of version 3 of the GNU General Public
License, supplemented by the additional permissions listed below.

  0. Additional Definitions.

  As used herein, "this License" refers to version 3 of the GNU Lesser
General Public License, and the "GNU GPL" refers to version 3 of the GNU
General Public License.

  "The Library" refers to a covered work governed by this License,
other than an Application or a Combined Work as defined below.

  An "Application" is any work that makes use of an interface provided
by the Library, but which is not otherwise based on the Library.
Defining a subclass of a class defined by the Library is deemed a mode
of using an interface provided by the Library.

  A "Combined Work" is a work produced by combining or linking an
Application with the Library.  The particular version of the Library
with which the Combined Work was made is also called the "Linked
Version".

  The "Minimal Corresponding Source" for a Combined Work means the
Corresponding Source for the Combined Work, excluding any source code
for portions of the Combined Work that, considered in isolation, are
based on the Application, and not on the Linked Version.

  The "Corresponding Application Code" for a Combined Work means the
object code and/or source code for the Application, including any data
and utility programs needed for reproducing the Combined Work from the
Application, but excluding the System Libraries of the Combined Work.

  1. Exception to Section 3 of the GNU GPL.

  You may convey a covered work under sections 3 and 4 of this License
without being bound by section 3 of the GNU GPL.

  2. Conveying Modified Versions.

  If you modify a copy of the Library, and, in your modifications, a
facility refers to a function or data to be supplied by an Application
that uses the facility (other than as an argument passed when the
facility is invoked), then you may convey a copy of the modified
version:

   a) under this License, provided that you make a good faith effort to
   ensure that, in the event an Application does not supply the
   function or data, the facility still operates, and performs
   whatever part of its purpose remains meaningful, or

   b) under the GNU GPL, with none of the additional permissions of
   this License applicable to that copy.

  3. Object Code Incorporating Material from Library Header Files.

  The object code form of an Application may incorporate material from
a header file that is part of the Library.  You may convey such object
code under terms of your choice, provided that, if the incorporated
material is not limited to numerical parameters, data structure
layouts and accessors, or small macros, inline functions and templates
(ten or fewer lines in length), you do both of the following:

   a) Give prominent notice with each copy of the object code that the
   Library is used in it and that the Library and its use are
   covered by this License.

   b) Accompany the object code with a copy of the GNU GPL and this license
   document.

  4. Combined Works.

  You may convey a Combined Work under terms of your choice that,
taken together, effectively do not restrict modification of the
portions of the Library contained in the Combined Work and reverse
engineering for debugging such modifications, if you also do each of
the following:

   a) Give prominent notice with each copy of the Combined Work that
   the Library is used in it and that the Library and its use are
   covered by this License.

   b) Accompany the Combined Work with a copy of the GNU GPL and this license
   document.

   c) For a Combined Work that displays copyright notices during
   execution, include the copyright notice for the Library among
   these notices, as well as a reference directing the user to the
   copies of the GNU GPL and this license document.

   d) Do one of the following:

       0) Convey the Minimal Corresponding Source under the terms of this
       License, and the Corresponding Application Code in a form
       suitable for, and under terms that permit, the user to
       recombine or relink the Application with a modified version of
       the Linked Version to produce a modified Combined Work, in the
       manner specified by section 6 of the GNU GPL for conveying
       Corresponding Source.

       1) Use a suitable shared library mechanism for linking with the
       Library.  A suitable mechanism is one that (a) uses at run time
       a copy of the Library already present on the user's computer
       system, and (b) will operate properly with a modified version
       of the Library that is interface-compatible with the Linked
       Version.

   e) Provide Installation Information, but only if you would otherwise
   be required to provide such information under section 6 of the
   GNU GPL, and only to the extent that such information is
   necessary to install and execute a modified version of the
   Combined Work produced by recombining or relinking the
   Application with a modified version of the Linked Version. (If
   you use option 4d0, the Installation Information must accompany
   the Minimal Corresponding Source and Corresponding Application
   Code. If you use option 4d1, you must provide the Installation
   Information in the manner specified by section 6 of the GNU GPL
   for conveying Corresponding Source.)

  5. Combined Libraries.

  You may place library facilities that are a work based on the
Library side by side in a single library together with other library
facilities that are not Applications and are not covered by this
License, and convey such a combined library under terms of your
choice, if you do both of the following:

   a) Accompany the combined library with a copy of the same work based
   on the Library, uncombined with any other library facilities,
   conveyed under the terms of this License.

   b) Give prominent notice with the combined library that part of it
   is a work based on the Library, and explaining where to find the
   accompanying uncombined form of the same work.

  6. Revised Versions of the GNU Lesser General Public License.

  The Free Software Foundation may publish revised and/or new versions
of the GNU Lesser General Public License from time to time. Such new
versions will be similar in spirit to the present version, but may
differ in detail to address new problems or concerns.

  Each version is given a distinguishing version number. If the
Library as you received it specifies that a certain numbered version
of the GNU Lesser General Public License "or any later version"
applies to it, you have the option of following the terms and
conditions either of that published version or of any later version
published by the Free Software Foundation. If the Library as you
received it does not specify a version number of the GNU Lesser
General Public License, you may choose any version of the GNU Lesser
General Public License ever published by the Free Software Foundation.

  If the Library as you received it specifies that a proxy can decide
whether future versions of the GNU Lesser General Public License shall
apply, that proxy's public statement of acceptance of any version is
permanent authorization for you to choose that version for the
Library.

*/