
#include <stdint.h>
#include <stdio.h>
#include "erl_nif.h"

#ifdef DEBUG
#include <assert.h>
#define DBG(X)
#define ASSERT(X) assert(X)
#else
#define DBG(X)
#define ASSERT(X)
#endif


static ERL_NIF_TERM read_rows(ErlNifEnv* env, ERL_NIF_TERM bin_term, char *data, unsigned data_size, int* types, unsigned type_size);
static inline int convert(ErlNifEnv* env, ERL_NIF_TERM bin_term, char* data_start, char *col_value, int32_t col_size, int type, ERL_NIF_TERM *term);
static inline int convert_int(ErlNifEnv* env, char *col_value, int32_t col_size, ERL_NIF_TERM *term);

static ERL_NIF_TERM read_rows_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  ErlNifBinary bin;
  ERL_NIF_TERM type_list = argv[1];
  ERL_NIF_TERM type_head;
  ERL_NIF_TERM type_rest;
  if (!enif_inspect_binary(env, argv[0], &bin)) {
    return enif_make_badarg(env);
  }

  unsigned len;
  if (!enif_get_list_length(env, argv[1], &len)) {
    return enif_make_badarg(env);
  } else {
    int types[len];
    for (unsigned i = 0; i < len; i++) {
      if (!enif_get_list_cell(env, type_list, &type_head, &type_rest)) {
        return enif_make_badarg(env);
      }
      if (!enif_get_int(env, type_head, types + i)) {
        return enif_make_badarg(env);
      }
      type_list = type_rest;
    }

    ERL_NIF_TERM ret = read_rows(env, argv[0], (char*)bin.data, bin.size, types, len);

    if (!ret) {
      return enif_make_badarg(env);
    }
    return ret;
  }
}

static inline int64_t read64(char* data) {
  int64_t value = *((int64_t*)data);
  return __builtin_bswap64(value);
}
static inline int32_t read32(char* data) {
  int32_t value = *((int32_t*)data);
  return __builtin_bswap32(value);
}
static inline int16_t read16(char* data) {
  int16_t value = *((int16_t*)data);
  return __builtin_bswap16(value);
}
static inline int8_t read8(char* data) {
  int8_t value = data[0];
  return value;
}

ERL_NIF_TERM read_rows(ErlNifEnv* env, ERL_NIF_TERM bin_term, char *data, unsigned data_size, int* types, unsigned type_size)
{
  char* data_start = data;
  char* data_end = data + data_size;
  if (data_size < 4) {
    return 0;
  }

  int32_t row_count = read32(data);
  data += 4;

  {
    ERL_NIF_TERM results[row_count][type_size];

    // Actual conversion starts
    for (int32_t r = 0; r < row_count; r++) {
      // Each row has `type_size` columns
      for (int32_t t = 0; t < type_size; t++) {
        if (data + 4 >= data_end) {
          return 0;
        }

        int32_t col_size = read32(data);
        data += 4;

        if (data + col_size > data_end) {
          return 0;
        }

        if (!convert(env, bin_term, data_start, data, col_size, types[t], &results[r][t])) {
          return 0;
        }

        if (col_size > 0) {
          data += col_size;
        }
      }
    }

    // Conversion is done, transfer to erlang lists:
    ERL_NIF_TERM rows[row_count];
    for (int32_t r = 0; r < row_count; r++) {
      rows[r] = enif_make_list_from_array(env, results[r], type_size);
    }

    ERL_NIF_TERM final = enif_make_list_from_array(env, rows, row_count);
    ERL_NIF_TERM rest = enif_make_sub_binary(env, bin_term, data - data_start, data_size - (data - data_start));

    return enif_make_tuple2(env, final, rest);
  }
}

int convert_int(ErlNifEnv* env, char *col_value, int32_t col_size, ERL_NIF_TERM *term) {
  switch(col_size) {
    case 8:
      *term = enif_make_int64(env, read64(col_value));
      break;
    case 4:
      *term = enif_make_int(env, read32(col_value));
      break;
    case 2:
      *term = enif_make_int(env, read16(col_value));
      break;
    case 1: {
      *term = enif_make_int(env, read8(col_value));
      break;
    }
    default:
      return 0;
  }
  return 1;
}

int convert(ErlNifEnv* env, ERL_NIF_TERM bin_term, char* data_start, char *col_value, int32_t col_size, int type, ERL_NIF_TERM *term) {

  if (col_size < 0) { // undefined
    *term = enif_make_atom(env, "undefined");
    return 1;
  }

  switch(type) {
    case 0x01:
      // Ascii (binary)
      *term = enif_make_sub_binary(env, bin_term, col_value - data_start, col_size);
      break;
    case 0x02:
      // bigint - 64
      return convert_int(env, col_value, col_size, term);
    case 0x03:
      // Blob - binary
      *term = enif_make_sub_binary(env, bin_term, col_value - data_start, col_size);
      break;
    case 0x04:
      // Boolean
      *term = enif_make_atom(env, *col_value == '1' ? "true" : "false");
      break;
    case 0x05:
      // Counter
      return convert_int(env, col_value, col_size, term);
    case 0x06:
      // Decimal
      // NOT IMPLEMENTED
      DBG(type);
      return 0;
    case 0x07: {
      // Double - 8 byte
      ASSERT(sizeof(double) == 8);
      if (col_size != 8) {
        return 0;
      }
      int64_t intVal = read64(col_value);
      double value = *((double*)&intVal);
      *term = enif_make_double(env, value);
      break;
    }
    case 0x08: {
      // Float - 4 byte
      ASSERT(sizeof(float) == 4);
      if (col_size != 4) {
        return 0;
      }
      int32_t intVal = read32(col_value);
      float value = *((float*)&intVal);
      *term = enif_make_double(env, value);
      break;
    }
    case 0x09:
      // Int
      return convert_int(env, col_value, col_size, term);
    case 0x0A:
      // Undefined in protocol V4?!
      DBG(type);
      return 0;
    case 0x0B:
      // Timestamp
      return convert_int(env, col_value, col_size, term);
    case 0x0C:
      // Uuid - binary
      *term = enif_make_sub_binary(env, bin_term, col_value - data_start, col_size);
      break;
    case 0x0D:
      // Varchar - binary
      *term = enif_make_sub_binary(env, bin_term, col_value - data_start, col_size);
      break;
    case 0x0E:
      // Varint
      return convert_int(env, col_value, col_size, term);
    case 0x0F:
      // Timeuuid
      // NOT IMPLEMENTED
      DBG(type);
      return 0;
    case 0x10:
      // Inet
      // NOT IMPLEMENTED
    case 0x11:
    case 0x12:
      // Undefined in protocol V4?!
      DBG(type);
      return 0;
    case 0x13:
      // Smallint
      return convert_int(env, col_value, col_size, term);
    case 0x14:
      // Tinyint
      return convert_int(env, col_value, col_size, term);
    case 0x20:
      // List TBD
    case 0x21:
      // Map TBD
    case 0x22:
      // Set TBD
    default:
      DBG(type);
      return 0;
  }

  return 1;
}

static ErlNifFunc nif_funcs[] = {
    {"read_rows", 2, read_rows_nif}
};

int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info)
{
  return 0;
}

int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info)
{
  return 0;
}

void unload(ErlNifEnv* env, void* priv_data)
{
}


ERL_NIF_INIT(ecql_decoding_2_0, nif_funcs, load, NULL, upgrade, unload);

