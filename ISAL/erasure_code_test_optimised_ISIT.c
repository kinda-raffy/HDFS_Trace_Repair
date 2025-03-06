/**********************************************************************
	Copyright(c) 2011-2015 Intel Corporation All rights reserved.

	Redistribution and use in source and binary forms, with or without
	modification, are permitted provided that the following conditions
	are met:
		* Redistributions of source code must retain the above copyright
			notice, this list of conditions and the following disclaimer.
		* Redistributions in binary form must reproduce the above copyright
			notice, this list of conditions and the following disclaimer in
			the documentation and/or other materials provided with the
			distribution.
		* Neither the name of Intel Corporation nor the names of its
			contributors may be used to endorse or promote products derived
			from this software without specific prior written permission.

	THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
	"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
	LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
	A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
	OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
	SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
	LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
	DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
	THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
	(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
	OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h> // for memset, memcmp
#include "erasure_code.h"
#include "types.h"
#include <time.h>
#include <math.h>
// #include "repair_scheme_tbl.h"
// #include "repair_scheme_tbl_9_6.h"
// #include "repair_scheme_tbl_OLD_Dung.h"
#include "repair_scheme_tbl_9_6_ISIT.h"
// #include "repair_scheme_tbl_11_8_ISIT.h"
// #include "repair_scheme_tbl_16_13_ISIT.h"
// #include "repair_scheme_tbl_12_8_ISIT.h"
// #include "repair_scheme_tbl_14_10_ISIT.h"
// #include "repair_scheme_tbl_16_12_ISIT.h"

#define TEST_SOURCES 127
#define TEST_LEN 10000000
#define BIN_LEN 8
// 100.000 = 1.2->1.5s
// 0.015
// end add

#define TEST_SIZE (TEST_LEN / 2)

#ifndef TEST_SOURCES
#define TEST_SOURCES 127
#endif
#ifndef RANDOMS
#define RANDOMS 100
#endif

#define MMAX TEST_SOURCES
#define KMAX TEST_SOURCES

#define EFENCE_TEST_MIN_SIZE 16
#define EFENCE_TEST_MAX_SIZE EFENCE_TEST_MIN_SIZE + 0x100

#ifdef EC_ALIGNED_ADDR
// Define power of 2 range to check ptr, len alignment
#define PTR_ALIGN_CHK_B 0
#define LEN_ALIGN_CHK_B 0 // 0 for aligned only
#else
// Define power of 2 range to check ptr, len alignment
#define PTR_ALIGN_CHK_B 32
#define LEN_ALIGN_CHK_B 32 // 0 for aligned only
#endif

#ifndef TEST_SEED
#define TEST_SEED 11
#endif

/**********************************************************************
	Copyright(c) 2011-2015 Intel Corporation All rights reserved.

	Redistribution and use in source and binary forms, with or without
	modification, are permitted provided that the following conditions
	are met:
		* Redistributions of source code must retain the above copyright
			notice, this list of conditions and the following disclaimer.
		* Redistributions in binary form must reproduce the above copyright
			notice, this list of conditions and the following disclaimer in
			the documentation and/or other materials provided with the
			distribution.
		* Neither the name of Intel Corporation nor the names of its
			contributors may be used to endorse or promote products derived
			from this software without specific prior written permission.

	THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
	"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
	LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
	A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
	OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
	SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
	LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
	DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
	THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
	(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
	OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/

#include <limits.h>
#include <string.h> // for memset
#include "erasure_code.h"
#include "ec_base.h" // for GF tables'#include<sys/time.

void ec_init_tables(int k, int rows, unsigned char *a, unsigned char *g_tbls)
{
	int i, j;

	for (i = 0; i < rows; i++)
	{
		for (j = 0; j < k; j++)
		{
			gf_vect_mul_init(*a++, g_tbls); // gf_vect_mul_inti: Calculates const table gftbl in GF(2^8) from single input A
			g_tbls += 32;
		}
	}
}

unsigned char gf_mul(unsigned char a, unsigned char b)
{
#ifndef GF_LARGE_TABLES
	int i;

	if ((a == 0) || (b == 0))
		return 0;

	return gff_base[(i = gflog_base[a] + gflog_base[b]) > 254 ? i - 255 : i];
#else
	return gf_mul_table_base[b * 256 + a];
#endif
}

unsigned char gf_inv(unsigned char a)
{
#ifndef GF_LARGE_TABLES
	if (a == 0)
		return 0;

	return gff_base[255 - gflog_base[a]];
#else
	return gf_inv_table_base[a];
#endif
}

void gf_gen_rs_matrix(unsigned char *a, int m, int k)
{
	int i, j;
	unsigned char p, gen = 1;

	memset(a, 0, k * m);
	for (i = 0; i < k; i++)
		a[k * i + i] = 1;

	for (i = k; i < m; i++)
	{
		p = 1;
		for (j = 0; j < k; j++)
		{
			a[k * i + j] = p;
			p = gf_mul(p, gen);
		}
		gen = gf_mul(gen, 2);
	}
}

void gf_gen_cauchy1_matrix(unsigned char *a, int m, int k)
{
	int i, j;
	unsigned char *p;

	// Identity matrix in high position
	memset(a, 0, k * m);
	for (i = 0; i < k; i++)
		a[k * i + i] = 1;

	// For the rest choose 1/(i + j) | i != j
	p = &a[k * k];
	char i_values[] = {78, 10, 153, 214, 68, 147};
	char j_values[] = {0, 1, 152};

	for (int i_idx = 0; i_idx < 6; i_idx++)
	{
		i = i_values[i_idx];
		for (int j_idx = 0; j_idx < 3; j_idx++)
		{
			j = j_values[j_idx];
			*p++ = gf_inv(i ^ j);
		}
	}
}

int gf_invert_matrix(unsigned char *in_mat, unsigned char *out_mat, const int n)
{
	int i, j, k;
	unsigned char temp;

	// Set out_mat[] to the identity matrix
	for (i = 0; i < n * n; i++) // memset(out_mat, 0, n*n)
		out_mat[i] = 0;

	for (i = 0; i < n; i++)
		out_mat[i * n + i] = 1;

	// Inverse
	for (i = 0; i < n; i++)
	{
		// Check for 0 in pivot element
		if (in_mat[i * n + i] == 0)
		{
			// Find a row with non-zero in current column and swap
			for (j = i + 1; j < n; j++)
				if (in_mat[j * n + i])
					break;

			if (j == n) // Couldn't find means it's singular
				return -1;

			for (k = 0; k < n; k++)
			{ // Swap rows i,j
				temp = in_mat[i * n + k];
				in_mat[i * n + k] = in_mat[j * n + k];
				in_mat[j * n + k] = temp;

				temp = out_mat[i * n + k];
				out_mat[i * n + k] = out_mat[j * n + k];
				out_mat[j * n + k] = temp;
			}
		}

		temp = gf_inv(in_mat[i * n + i]); // 1/pivot
		for (j = 0; j < n; j++)
		{ // Scale row i by 1/pivot
			in_mat[i * n + j] = gf_mul(in_mat[i * n + j], temp);
			out_mat[i * n + j] = gf_mul(out_mat[i * n + j], temp);
		}

		for (j = 0; j < n; j++)
		{
			if (j == i)
				continue;

			temp = in_mat[j * n + i];
			for (k = 0; k < n; k++)
			{
				out_mat[j * n + k] ^= gf_mul(temp, out_mat[i * n + k]);
				in_mat[j * n + k] ^= gf_mul(temp, in_mat[i * n + k]);
			}
		}
	}
	return 0;
}

//
// gftbl(A) = {A{00}, A{01}, A{02}, ... , A{0f} 15 }, {A{00}, A{10}, A{20}, ... , A{f0} 240 }

void gf_vect_mul_init(unsigned char c, unsigned char *tbl)
{
	unsigned char c2 = (c << 1) ^ ((c & 0x80) ? 0x1d : 0);	 // Mult by GF{2}
	unsigned char c4 = (c2 << 1) ^ ((c2 & 0x80) ? 0x1d : 0); // Mult by GF{2}
	unsigned char c8 = (c4 << 1) ^ ((c4 & 0x80) ? 0x1d : 0); // Mult by GF{2}

#if __WORDSIZE == 64 || _WIN64 || __x86_64__
	// [MARK] What is going on here?
	unsigned long long v1, v2, v4, v8, *t;
	unsigned long long v10, v20, v40, v80;
	unsigned char c17, c18, c20, c24;

	t = (unsigned long long *)tbl;

	v1 = c * 0x0100010001000100ull;
	v2 = c2 * 0x0101000001010000ull;
	v4 = c4 * 0x0101010100000000ull;
	v8 = c8 * 0x0101010101010101ull;

	v4 = v1 ^ v2 ^ v4;
	t[0] = v4;
	t[1] = v8 ^ v4;

	c17 = (c8 << 1) ^ ((c8 & 0x80) ? 0x1d : 0);		// Mult by GF{2}
	c18 = (c17 << 1) ^ ((c17 & 0x80) ? 0x1d : 0); // Mult by GF{2}
	c20 = (c18 << 1) ^ ((c18 & 0x80) ? 0x1d : 0); // Mult by GF{2}
	c24 = (c20 << 1) ^ ((c20 & 0x80) ? 0x1d : 0); // Mult by GF{2}

	v10 = c17 * 0x0100010001000100ull;
	v20 = c18 * 0x0101000001010000ull;
	v40 = c20 * 0x0101010100000000ull;
	v80 = c24 * 0x0101010101010101ull;

	v40 = v10 ^ v20 ^ v40;
	t[2] = v40;
	t[3] = v80 ^ v40;

#else // 32-bit or other 16 bit
	unsigned char c3, c5, c6, c7, c9, c10, c11, c12, c13, c14, c15;
	unsigned char c17, c18, c19, c20, c21, c22, c23, c24, c25, c26, c27, c28, c29, c30,
			c31;

	c3 = c2 ^ c;
	c5 = c4 ^ c;
	c6 = c4 ^ c2;
	c7 = c4 ^ c3;

	c9 = c8 ^ c;
	c10 = c8 ^ c2;
	c11 = c8 ^ c3;
	c12 = c8 ^ c4;
	c13 = c8 ^ c5;
	c14 = c8 ^ c6;
	c15 = c8 ^ c7;

	tbl[0] = 0;
	tbl[1] = c;
	tbl[2] = c2;
	tbl[3] = c3;
	tbl[4] = c4;
	tbl[5] = c5;
	tbl[6] = c6;
	tbl[7] = c7;
	tbl[8] = c8;
	tbl[9] = c9;
	tbl[10] = c10;
	tbl[11] = c11;
	tbl[12] = c12;
	tbl[13] = c13;
	tbl[14] = c14;
	tbl[15] = c15;

	c17 = (c8 << 1) ^ ((c8 & 0x80) ? 0x1d : 0);
	/*if c8 > 128 -> XOR 29.
	 Else XOR 0

	 case 1: c8 << 1 XOR 29
	 case 2: c8 << 1 XOR 0*/
	c18 = (c17 << 1) ^ ((c17 & 0x80) ? 0x1d : 0); // Mult by GF{2}//29 = 0001 1101
	c19 = c18 ^ c17;
	c20 = (c18 << 1) ^ ((c18 & 0x80) ? 0x1d : 0); // Mult by GF{2}
	c21 = c20 ^ c17;
	c22 = c20 ^ c18;
	c23 = c20 ^ c19;
	c24 = (c20 << 1) ^ ((c20 & 0x80) ? 0x1d : 0); // Mult by GF{2}
	c25 = c24 ^ c17;
	c26 = c24 ^ c18;
	c27 = c24 ^ c19;
	c28 = c24 ^ c20;
	c29 = c24 ^ c21;
	c30 = c24 ^ c22;
	c31 = c24 ^ c23;

	tbl[16] = 0;
	tbl[17] = c17;
	tbl[18] = c18;
	tbl[19] = c19;
	tbl[20] = c20;
	tbl[21] = c21;
	tbl[22] = c22;
	tbl[23] = c23;
	tbl[24] = c24;
	tbl[25] = c25;
	tbl[26] = c26;
	tbl[27] = c27;
	tbl[28] = c28;
	tbl[29] = c29;
	tbl[30] = c30;
	tbl[31] = c31;

#endif //__WORDSIZE == 64 || _WIN64 || __x86_64__
}

void ec_encode_data_base(int len, int srcs, int dests, unsigned char *v,
												 unsigned char **src, unsigned char **dest)

// Len: Length of each block of data (vector) of source or dest data.
// srcs (k): The number of vector sources or rows in the generator matrix for coding
// dests (rows): The number of output vectors to concurrently encode/decode.
// v(g_tbls): Pointer to array of input tables generated from coding coefficients in ec_init_tables().
// Must be of size 32*k*rows
// src (data): Array of pointers to source input buffers.
// dest (coding): Array of pointers to coded output buffers.
{
	int i, j, l;
	unsigned char s;

	for (l = 0; l < dests; l++)
	{ // l = 1
		for (i = 0; i < len; i++)
		{ // i = 0
			s = 0;
			for (j = 0; j < srcs; j++)
			{
				s ^= gf_mul(src[j][i], v[j * 32 + l * srcs * 32 + 1]);
			}
			dest[l][i] = s;
			// printf("s: %u \n", s);
		}
	}
}

void ec_encode_data_update_base(int len, int k, int rows, int vec_i, unsigned char *v,
																unsigned char *data, unsigned char **dest)
{
	int i, l;
	unsigned char s;

	for (l = 0; l < rows; l++)
	{
		for (i = 0; i < len; i++)
		{
			s = dest[l][i];
			s ^= gf_mul(data[i], v[vec_i * 32 + l * k * 32 + 1]);

			dest[l][i] = s;
		}
	} //&buff[i]
}

struct slver
{
	unsigned short snum;
	unsigned char ver;
	unsigned char core;
};

// Version info
struct slver gf_vect_mul_init_slver_00020035;
struct slver gf_vect_mul_init_slver = {0x0035, 0x02, 0x00};

struct slver ec_encode_data_base_slver_00010135;
struct slver ec_encode_data_base_slver = {0x0135, 0x01, 0x00};

struct slver gf_vect_mul_base_slver_00010136;
struct slver gf_vect_mul_base_slver = {0x0136, 0x01, 0x00};

struct slver gf_vect_dot_prod_base_slver_00010137;
struct slver gf_vect_dot_prod_base_slver = {0x0137, 0x01, 0x00};

struct slver gf_mul_slver_00000214;
struct slver gf_mul_slver = {0x0214, 0x00, 0x00};

struct slver gf_invert_matrix_slver_00000215;
struct slver gf_invert_matrix_slver = {0x0215, 0x00, 0x00};

struct slver gf_gen_rs_matrix_slver_00000216;
struct slver gf_gen_rs_matrix_slver = {0x0216, 0x00, 0x00};

struct slver gf_gen_cauchy1_matrix_slver_00000217;
struct slver gf_gen_cauchy1_matrix_slver = {0x0217, 0x00, 0x00};

void ec_encode_data(int len, int srcs, int dests, unsigned char *v,
										unsigned char **src, unsigned char **dest)
{
	ec_encode_data_base(len, srcs, dests, v, src, dest);
}

typedef unsigned char u8;

void dump(unsigned char *buf, int len)
{
	int i;
	for (i = 0; i < len;)
	{
		printf(" %u", 0xff & buf[i++]);
		if (i % 32 == 0)
			printf("\n");
	}
	printf("\n");
}

void dump_matrix(unsigned char **s, int k, int m)
{
	int i, j;
	for (i = 0; i < k; i++)
	{
		for (j = 0; j < m; j++)
		{
			printf(" %u", s[i][j]);
		}
		printf("\n");
	}
	printf("\n");
}

void dump_u8xu8(unsigned char *s, int k, int m)
{
	int i, j;
	for (i = 0; i < k; i++)
	{
		for (j = 0; j < m; j++)
		{
			printf(" %u", 0xff & s[j + (i * m)]);
		}
		printf("\n");
	}
	printf("\n");
}

// Generate Random errors
static void gen_err_list(unsigned char *src_err_list,
												 unsigned char *src_in_err, int *pnerrs, int *pnsrcerrs, int k, int m)
{
	int i, err;
	int nerrs = 0, nsrcerrs = 0;

	for (i = 0, nerrs = 0, nsrcerrs = 0; i < m && nerrs < m - k; i++)
	{
		err = 1 & rand();
		src_in_err[i] = err;
		if (err)
		{
			src_err_list[nerrs++] = i;
			if (i < k)
			{
				nsrcerrs++;
			}
		}
	}
	if (nerrs == 0)
	{ // should have at least one error
		while ((err = (rand() % KMAX)) >= m)
			;
		src_err_list[nerrs++] = err;
		src_in_err[err] = 1;
		if (err < k)
			nsrcerrs = 1;
	}
	*pnerrs = nerrs;
	*pnsrcerrs = nsrcerrs; // number of data error
	return;
}

#define NO_INVERT_MATRIX -2
// Generate decode matrix from encode matrix
static int gf_gen_decode_matrix(unsigned char *encode_matrix,
																unsigned char *decode_matrix,
																unsigned char *invert_matrix,
																unsigned int *decode_index,
																unsigned char *src_err_list,
																unsigned char *src_in_err,
																int nerrs, int nsrcerrs, int k, int m)
{
	int i, j, p;
	int r;
	unsigned char *backup, *b, s;
	int incr = 0;

	b = malloc(MMAX * KMAX);
	// MALLOC: or “memory allocation” method in C is used to dynamically allocate
	// a single large block of memory with the specified size.
	// It returns a pointer of type void which can be cast into a pointer of any form.
	//  It initializes each block with default garbage value

	backup = malloc(MMAX * KMAX);

	if (b == NULL || backup == NULL)
	{
		printf("Test failure! Error with malloc\n");
		free(b);
		free(backup);
		// is used to dynamically de-allocate the memory.
		// The memory allocated using functions malloc() is not de-allocated on their own.
		// Hence the free() method is used. helps to reduce wastage of memory by freeing it
		return -1;
	}

	// Construct matrix b by removing error rows
	// printf("B AND BACKUP MATRIX: \n");
	for (i = 0, r = 0; i < k; i++, r++)
	{
		while (src_in_err[r])
			r++;
		for (j = 0; j < k; j++)
		{
			b[k * i + j] = encode_matrix[k * r + j];
			backup[k * i + j] = encode_matrix[k * r + j];
			/*printf("%u ", b[k * i + j]);
			if ((j%k)==k-1)
			printf("\n");*/
		}
		decode_index[i] = r; // non erased data location
	}
	incr = 0;

	while (gf_invert_matrix(b, invert_matrix, k) < 0)
	{
		if (nerrs == (m - k))
		{
			free(b);
			free(backup);
			printf("BAD MATRIX\n");
			return NO_INVERT_MATRIX;
		}
		incr++;
		memcpy(b, backup, MMAX * KMAX);
		for (i = nsrcerrs; i < nerrs - nsrcerrs; i++)
		{
			if (src_err_list[i] == (decode_index[k - 1] + incr))
			{
				// skip the erased parity line
				incr++;
				continue;
			}
		}
		if (decode_index[k - 1] + incr >= m)
		{
			free(b);
			free(backup);
			printf("BAD MATRIX\n");
			return NO_INVERT_MATRIX;
		}
		decode_index[k - 1] += incr;
		for (j = 0; j < k; j++)
			b[k * (k - 1) + j] = encode_matrix[k * decode_index[k - 1] + j];
	};

	// non parity decoding. eg recover data that has index < k
	for (i = 0; i < nsrcerrs; i++)
	{
		for (j = 0; j < k; j++)
		{
			decode_matrix[k * i + j] = invert_matrix[k * src_err_list[i] + j];
		}
	}
	/* src_err_list from encode_matrix * invert of b for parity decoding */
	for (p = nsrcerrs; p < nerrs; p++)
	{ // nserrs: number of erased data unit , nerrs: Erased index
		for (i = 0; i < k; i++)
		{
			s = 0;
			for (j = 0; j < k; j++)
				s ^= gf_mul(invert_matrix[j * k + i],
										encode_matrix[k * src_err_list[p] + j]);

			decode_matrix[k * p + i] = s;
		} // b is parity check matrix
	}
	free(b);
	free(backup);
	return 0;
}
/********************************************************************************/

// This version is based on Dung and Hoang's optimisation combined with Minh's idea: down to 3-5x
// In use

unsigned char *repair_trace_optimised_OLD(int n, int j, unsigned char **buffs)
{
	int a, b, i, u, idx, test_codeword; // for FOR loop
	unsigned char bw[n];								// bw[i] = #bits sent from Node i to Node j
	unsigned char table[72];						// to store a row in H or R to speed up the access
	unsigned char *Hij;									// Hij = H[i][j]+1 - ignoring the first element which is rank/bandwidth bi
	unsigned char *Rij;									// Rij = R[i][j]+1 - ignoring the first element which is rank/bandwidth bi
	unsigned char *Dj;									// Dj stores the dual basis used to recover Node j
	unsigned int traces_as_number;			// to store the decimal value of repair traces (converted to a number)

	unsigned char *RepairTr = (unsigned char *)malloc(sizeof(unsigned char) * n * 8 * TEST_LEN); // to store all actual repair traces as bits

	unsigned char *RepairTrAsNumbers = (unsigned char *)malloc(sizeof(unsigned char) * n * TEST_LEN); // to store repair traces as a number
	memset(RepairTrAsNumbers, 0, sizeof(unsigned char) * n * TEST_LEN);

	unsigned char *revMem = (unsigned char *)malloc(sizeof(unsigned char) * n * 256); // revMem is a compact array (no TEST_LEN), revMem[256*i+b] = contribution of Node i to the final sum
	memset(revMem, 0, sizeof(unsigned char) * n * 256);

	unsigned char *rev = (unsigned char *)malloc(sizeof(unsigned char) * TEST_LEN); // to store the recovery cj (xTEST_LEN)
	memset(rev, 0, sizeof(unsigned char) * TEST_LEN);

	//--------------------------------------------------------------
	// Start of senders' computation: Node i computes the repair/helper traces

	clock_t start = clock();

	// Calculate bandwidth using H table
	for (i = 0; i < n; i++)
	{
		if (i != j)
		{
			bw[i] = h_htbl[i][j][0];
		}
	}

	for (i = 0; i < n; i++)
	{
		if (i != j)
		{
			Hij = h_htbl[i][j] + 1; // Hij = &H[i][j][1]
			idx = i * 8 * TEST_LEN;
			for (a = 0; a < bw[i]; a++)
			{
				for (test_codeword = 0; test_codeword < TEST_LEN; test_codeword++)
				{
					RepairTr[idx++] = parity[Hij[a] & (buffs[i][test_codeword])]; // idx = i*8*TEST_LEN + a*TEST_LEN + test_codeword
				}
			}
		}
	}
	clock_t end = clock();
	double elapsed = (double)(end - start) / CLOCKS_PER_SEC;
	printf("Generate repair traces: %f secs \n", elapsed);
	// End of sender computation
	//-------------------------------------------------------------------

	//-------------------------------------------------------------------
	// Star of receiver computation: recovering cj from the helper/repair traces

	// First, transform repair traces into decimal numbers
	// RepairTrAsNumbers[i*TEST_LEN test_codewordu => decimal representation of repair traces from Node i, codeword test_codeword

	start = clock();
	int index_column_tr;

	for (i = 0; i < n; i++)
	{
		if (i != j)
		{
			idx = i * TEST_LEN;
			for (test_codeword = 0; test_codeword < TEST_LEN; test_codeword++)
			{
				traces_as_number = 0; // convert bi[i] bit traces to decimal
				// E.g., bw[i] = 4 and repair traces are (b0,b1,b2,b3) = (0,1,1,1) --> 7
				for (a = 0; a < bw[i]; a++)
				{
					// for (a = bw[i]-1; a >= 0; a--){
					traces_as_number = traces_as_number << 1;
					traces_as_number ^= RepairTr[i * 8 * TEST_LEN + a * TEST_LEN + test_codeword];
				}
				RepairTrAsNumbers[idx++] = traces_as_number; // idx = i*TEST_LEN + test_codeword
																										 // RepairTrAsNumbers[i*TEST_LEN + test_codeword] = traces_as_number;
			}
		}
	}
	end = clock();
	elapsed = (double)(end - start) / CLOCKS_PER_SEC;
	printf("Retrieve repair traces as numbers: %f secs \n", elapsed);

	//---------------------------------------------------
	// Next, use the decimal repair traces to recover cj

	start = clock();
	// revMem is a compact array (no TEST_LEN), revMem[256*i+b] = contribution of Node i to the final sum including dual basis factors (sum by column i rather than sum by row)

	Dj = h_dtbl[j];
	for (i = 0; i < n; i++)
	{
		if (i != j)
		{
			Rij = h_RTable[i][j] + 1; // ignoring the bandwidth in the first entry
			for (b = 0; b < 256; b++)
			{
				for (a = 0; a < 8; a++)
				{
					// revMem[idx] ^= (parity[Rij[a] & b]) * Dj[a]; 	// idx = 256*i + b
					revMem[(i << 8) + b] ^= (parity[Rij[a] & b]) * Dj[a]; // idx = 256*i + b
				}
				// idx++;
			}
		}
	}

	for (i = 0; i < n; i++)
	{
		if (i != j)
		{
			for (test_codeword = 0; test_codeword < TEST_LEN; test_codeword++)
			{
				idx = i * TEST_LEN + test_codeword;
				traces_as_number = RepairTrAsNumbers[idx];
				rev[test_codeword] ^= revMem[(i << 8) + traces_as_number];
			}
		}
	}

	end = clock();
	elapsed = (double)(end - start) / CLOCKS_PER_SEC;
	printf("Recover cj: %f secs \n", elapsed);
	// End of receiver's computation
	//----------------------------------------------------

	return rev;
}

// This version is based on Dung and Hoang's optimisation combined with Minh's idea: down to 3-5x
// A more realistic version separating the helpers & receiver

// Sender i: returns the array repairTr containing all TEST_LEN x bw[i] bits as repair traces

unsigned char *repair_trace_generation(int n, int i, int j, unsigned char **buffs)
{
	int a, b, t, idx, test_codeword; // for FOR loop
	unsigned char bw;								 // bw[i] = #bits sent from Node i to Node j
	unsigned char *Hij;							 // Hij = H[i][j]+1 - ignoring the first element which is rank/bandwidth bi

	// Check if i <> j
	if (i == j)
	{
		printf("ERROR: i = j");
		return NULL;
	}

	// Calculate bandwidth using H table
	bw = h_htbl[i][j][0];
	unsigned char *RepairTr = (unsigned char *)malloc(sizeof(unsigned char) * bw * TEST_LEN); // to store all actual repair traces as bits

	Hij = h_htbl[i][j] + 1; // Hij = &H[i][j][1]: ignoring the first element which is the bw
	idx = 0;
	for (a = 0; a < bw; a++)
	{
		for (test_codeword = 0; test_codeword < TEST_LEN; test_codeword++)
		{
			RepairTr[idx++] = parity[Hij[a] & (buffs[i][test_codeword])]; // idx = a*TEST_LEN + test_codeword
																																		// RepairTr[idx++] = product[Hij[a]][(buffs[i][test_codeword])];
		}
	}

	return RepairTr;
}

// Receiver j: receives repair traces from other Node i and recover cj (TEST_LEN of them)

// buffs is the data.
unsigned char *repair_trace_optimised(int n, int j, unsigned char **buffs, double *repair_time_ptr)
{
	int a, b, i, u, idx, test_codeword;							// for FOR loop
	unsigned char bw[n];														// bw[i] = #bits sent from Node i to Node j
	unsigned char table[72];												// to store a row in H or R to speed up the access
	unsigned char *Rij;															// Rij = R[i][j]+1 - ignoring the first element which is rank/bandwidth bi
	unsigned char *Dj;															// Dj stores the dual basis used to recover Node j
	unsigned int traces_as_number;									// to store the decimal value of repair traces (converted to a number)
	clock_t start, end;															// to measure the running time
	double elapsed_array[n], elapsed, max_run_time; // to store the running times and max running time of n-1 helpers

	unsigned char *RepairTrArray[n];																																	// RepairTrArray[i] is the pointer to the traces sent from Node i
	unsigned char *RepairTrAsNumbers = (unsigned char *)malloc(sizeof(unsigned char) * n * TEST_LEN); // to store repair traces as a number
	memset(RepairTrAsNumbers, 0, sizeof(unsigned char) * n * TEST_LEN);

	unsigned char *revMem = (unsigned char *)malloc(sizeof(unsigned char) * n * 256); // revMem is a compact array (no TEST_LEN), revMem[256*i+b] = contribution of Node i to the final sum
	memset(revMem, 0, sizeof(unsigned char) * n * 256);

	unsigned char *rev = (unsigned char *)malloc(sizeof(unsigned char) * TEST_LEN); // to store the recovery cj (xTEST_LEN)
	memset(rev, 0, sizeof(unsigned char) * TEST_LEN);

	//--------------------------------------------------------------
	// Start of senders' computation: Node i computes the repair/helper traces
	max_run_time = 0;
	for (i = 0; i < n; i++)
	{
		if (i != j)
		{
			start = clock();
			RepairTrArray[i] = repair_trace_generation(n, i, j, buffs);
			end = clock();
			elapsed = (double)(end - start) / CLOCKS_PER_SEC;
			elapsed_array[i] = elapsed;
			if (max_run_time < elapsed)
			{
				max_run_time = elapsed;
			}
		}
	}
	// Use max running time among the helpers to make the waiting time realistic
	printf("(Max) Generate repair traces: %f secs \n", max_run_time);
	// End of sender computation
	//-------------------------------------------------------------------

	//-------------------------------------------------------------------
	// Start of receiver computation: recovering cj from the helper/repair traces

	// First, transform repair traces into decimal numbers
	// RepairTrAsNumbers[i*TEST_LEN test_codewordu => decimal representation of repair traces from Node i, codeword test_codeword

	start = clock();
	int index_column_tr;

	// Retrieve bandwidths for different helper nodes using R table
	for (i = 0; i < n; i++)
	{
		bw[i] = h_RTable[i][j][0];
	}

	for (i = 0; i < n; i++)
	{
		if (i != j)
		{
			idx = i * TEST_LEN;
			for (test_codeword = 0; test_codeword < TEST_LEN; test_codeword++)
			{
				traces_as_number = 0; // convert bi[i] bit traces to decimal
				// E.g., bw[i] = 4 and repair traces are (b0,b1,b2,b3) = (0,1,1,1) --> 7
				for (a = 0; a < bw[i]; a++)
				{
					// for (a = bw[i]-1; a >= 0; a--){
					traces_as_number = traces_as_number << 1;
					traces_as_number ^= RepairTrArray[i][a * TEST_LEN + test_codeword];
				}
				RepairTrAsNumbers[idx++] = traces_as_number; // idx = i*TEST_LEN + test_codeword
																										 // RepairTrAsNumbers[i*TEST_LEN + test_codeword] = traces_as_number;
			}
		}
	}
	end = clock();
	double elapsed_decimal = (double)(end - start) / CLOCKS_PER_SEC;
	printf("Retrieve repair traces as numbers: %f secs \n", elapsed_decimal);

	//---------------------------------------------------
	// Next, use the decimal repair traces to recover cj

	start = clock();
	// revMem is a compact array (no TEST_LEN), revMem[256*i+b] = contribution of Node i to the final sum including dual basis factors (sum by column i rather than sum by row)

	Dj = h_dtbl[j];
	for (i = 0; i < n; i++)
	{
		if (i != j)
		{
			Rij = h_RTable[i][j] + 1; // Ignoring the bandwidth in the first entry.
			for (b = 0; b < 256; b++)
			{
				for (a = 0; a < 8; a++)
				{
					// revMem[idx] ^= (parity[Rij[a] & b]) * Dj[a]; 	// idx = 256*i + b
					revMem[(i << 8) + b] ^= (parity[Rij[a] & b]) * Dj[a]; // idx = 256*i + b
				}
				// idx++;
			}
		}
	}

	// [MARK] Is this where cj is constructed?
	for (i = 0; i < n; i++)
	{
		if (i != j)
		{
			for (test_codeword = 0; test_codeword < TEST_LEN; test_codeword++)
			{
				idx = i * TEST_LEN + test_codeword;
				traces_as_number = RepairTrAsNumbers[idx];
				rev[test_codeword] ^= revMem[(i << 8) + traces_as_number];
			}
		}
	}

	end = clock();
	double elapsed_rev = (double)(end - start) / CLOCKS_PER_SEC;
	printf("Recover cj: %f secs \n", elapsed_rev);

	printf("-----------------------------------------\n");
	double trace_repair_time_accurate = max_run_time + elapsed_decimal + elapsed_rev;
	printf("Trace repair ACCURATE (per erasure): %.6f seconds\n", trace_repair_time_accurate);
	printf("-----------------------------------------\n");
	*repair_time_ptr = trace_repair_time_accurate;

	// End of receiver's computation
	//----------------------------------------------------

	return rev;
}

// parity[m] = XOR sum of its bits, used for compute dot product of two integers as 8-bit vectors
// Minh's function
void precompute()
{
	int i;
	parity[0] = 0;
	for (i = 1; i < 256; i++)
	{
		parity[i] = parity[i >> 1] ^ (i & 1);
	}
}

// Generate Random errors
// Rewritten by Hoang to generate a single erasure for our purpose
static void gen_err_list_single_erasure(unsigned char *src_err_list,
																				unsigned char *src_in_err, int *pnerrs, int *pnsrcerrs, int k, int m)
{
	int i, err;
	int nerrs = 0, nsrcerrs = 0;

	nerrs = 1;						 // number of erasure = 1
	err = rand() % m;			 // err can be a random position between 0 and m-1
	src_err_list[0] = err; // record the error position (only one)

	nsrcerrs = 0; // count if a source/data position is erased or not
	if (err < k)
	{
		nsrcerrs = 1;
	}

	for (i = 0; i < m; i++)
		src_in_err[i] = 0;
	src_in_err[err] = 1; // set the reverse array: src_in_err[i] = 1 if and only if ci is erased

	*pnerrs = nerrs;
	*pnsrcerrs = nsrcerrs; // number of data error
	return;
}

int main(int argc, char *argv[])
{
	// [MARK] What is re, rtest, m, k?
	int re = 0;
	int i, j, p, rtest, m, k;
	// mk 96 scheme k is file size 6ytes.
	// [MARK] nerrs, nsrcerrs is the count of faulty nodes?
	int nerrs, nsrcerrs;
	void *buf;
	unsigned int decode_index[MMAX];
	unsigned char *temp_buffs[TEST_SOURCES], *buffs[TEST_SOURCES];
	unsigned char *encode_matrix, *decode_matrix, *invert_matrix, *g_tbls;
	unsigned char src_in_err[TEST_SOURCES], src_err_list[TEST_SOURCES];
	unsigned char *recov[TEST_SOURCES];

	int rows, align, size;
	unsigned char *efence_buffs[TEST_SOURCES];
	unsigned int offset;
	u8 *ubuffs[TEST_SOURCES];
	u8 *temp_ubuffs[TEST_SOURCES];

	printf("erasure_code_test: %dx%d \n", TEST_SOURCES, TEST_LEN);
	// To make random data change with different calls.
	srand(time(0));
	// srand(TEST_SEED);

	// Allocate the arrays.
	for (i = 0; i < TEST_SOURCES; i++)
	{
		if (posix_memalign(&buf, 64, TEST_LEN))
		{
			printf("alloc error: Fail");
			return -1;
		}
		buffs[i] = buf;
	}

	for (i = 0; i < TEST_SOURCES; i++)
	{
		if (posix_memalign(&buf, 64, TEST_LEN))
		{
			printf("alloc error: Fail");
			return -1;
		}
		temp_buffs[i] = buf;
	}

	// Test erasure code by encode and recovery

	encode_matrix = malloc(MMAX * KMAX);
	decode_matrix = malloc(MMAX * KMAX);
	invert_matrix = malloc(MMAX * KMAX);
	g_tbls = malloc(KMAX * TEST_SOURCES * 32);
	if (encode_matrix == NULL || decode_matrix == NULL || invert_matrix == NULL || g_tbls == NULL)
	{
		printf("Test failure! Error with malloc\n");
		return -1;
	};

	m = 9;
	k = 6;
	printf("Comparing ISA-L and Trace Repair for n = %d and k = %d \n\n", m, k);

	if (m > MMAX || k > KMAX)
		return -1; // exit

	// printf("RANDOM DATA: \n");
	for (i = 0; i < k; i++)
	{ // i is the dataword lengths
		for (j = 0; j < TEST_LEN; j++)
		{
			buffs[i][j] = rand();
		}
	}

	// printf("\n");
	//  Generate encode matrix encode_matrix
	//  The matrix generated by gf_gen_rs_matrix
	gf_gen_cauchy1_matrix(encode_matrix, m, k);
	// output: encode_matrix

	/*printf("GENERATOR MATRIX: \n");
	for(i = 0 ; i < k*m ; i++)
	{
		printf("%u ",encode_matrix[i]);
		if((i % k) == k-1)
		printf("\n");
	}
	printf("\n");*/

	// Generate g_tbls from encode matrix
	ec_init_tables(k, m - k, &encode_matrix[k * k], g_tbls);
	// gf_vect_mul_init: Calculates const table gftbl in GF(2^8) from single input A
	// m - k = rows = the number of output vector, e.g., 9 - 5 = 4
	//&encode_matrix[k * k]: Pointer to sets of arrays of input coefficients used to encode or decode data
	// g_tbls: Pointer to start of space for concatenated output tables
	// generated from input coefficients.  Must be of size 32*k*rows

	/*printf("Initial table: \n");
	for (i = 0; i < (32*(k*(m-k))); i++)
	{
		printf("%u ", g_tbls[i]);
		if ((i % 32) == 31)
		printf("\n");
	}*/

	// Perform matrix dot_prod for EC encoding
	// using g_tbls from encode matrix encode_matrix

	ec_encode_data(TEST_LEN, k, m - k, g_tbls, buffs, &buffs[k]);

	/* TEST_LEN: Length of each block of data (vector) of source data
	k: The number of vector source in G matrix
	m-k: The number of output vectors to concurrently encode/decode.
	gftbls: Pointer to array of input tables generated from coding
	coefficients in ec_init_tables(). Must be of size 32*k*rows
	buffs (data): Array of pointers to source input buffers
	&buffs[k]: Arrays of pointer to coded output buffers*/
	/*printf("\n");
	printf("ENCODED DATA: \n");
	for (i = 0; i < m; i++){
		for (j = 0; j < TEST_LEN; j++){
			printf("%u ", buffs[i][j]);
			if((j % TEST_LEN ) == TEST_LEN-1)
			printf("\n");
		}
	}
	*/

	// Choose random buffers to be in erasure
	memset(src_in_err, 0, TEST_SOURCES);
	// Fill whole src_in_err array with 0
	// TEST_SOURCE: Number of byte to be filled starting from src_in_err to be filled

	// Add errorsposix_memalign
	// HOANG: USE A DIFFERENT FUNCTION TO GENERATE A SINGLE ERASURE INSTEAD
	// gen_err_list() originally can generate between 1 and 3 erasures
	gen_err_list_single_erasure(src_err_list, src_in_err, &nerrs, &nsrcerrs, k, m);

	//------------------------------------------------------------------
	// BEGINNING OF TRACE REPAIR (OUR METHOD)

	clock_t start = clock();
	unsigned char *rev;
	int test_codeword;

	// Precompute the parity array to speed up the computation
	precompute();
	double trace_repair_time_accurate;
	for (int err = 0; err < nerrs; err++)
	{
		j = src_err_list[err];
		rev = repair_trace_optimised(m, j, buffs, &trace_repair_time_accurate);

		// Compare recovered data with original data
		// this is check step for repair_trace_optimised
		for (test_codeword = 0; test_codeword < TEST_LEN; test_codeword++)
		{
			if (rev[test_codeword] != buffs[j][test_codeword])
			{
				printf("ERROR: TraceRepair doesn't match\n");
				exit(1);
			}
		}
	}
	clock_t finish = clock();

	printf("-----------------------------------------\n");
	double trace_repair_time = ((double)(finish - start) / CLOCKS_PER_SEC);
	printf("Trace repair (per erasure): %.6f seconds\n", trace_repair_time);
	// printf("Run time (Rpair Trace): %d micro seconds\n",elapsed);
	printf("-----------------------------------------\n");
	// END OF TRACE REPAIR

	//--------------------------------------------------------------------------
	// Beginning of ISA-L repair

	clock_t tic = clock();
	// Generate decode matrix from encode matrix
	re = gf_gen_decode_matrix(encode_matrix, decode_matrix,
														invert_matrix, decode_index, src_err_list, src_in_err,
														nerrs, nsrcerrs, k, m);
	if (re != 0)
	{
		printf("Fail to gf_gen_decode_matrix\n");
		return -1;
	}

	// printf("\n");
	/*
	//Print error list and non-erasure index
	printf(" - erase list = ");
	for (j = 0; j < nerrs; j++)
		printf(" %d", src_err_list[j]);
	printf("\n - Index = ");
	for (p = 0; p < k; p++){
		printf(" %d", decode_index[p]);
	}*/
	/*
	printf("\n \n");
	printf("nsrcerrs (number of original data error): %u \n", nsrcerrs);
	//mean the data are erasured that is in original data. Eg: lost data where<k.
	printf("nerrs (number of erased data): %u \n", nerrs);
	printf("\n");


	//Generate invert matrix
	printf("INVERT MATRIX: \n");
	for (i = 0; i < k*k; i++){
		printf(" %u", invert_matrix[i]);
		if ((i%k) == k-1)
		printf("\n");
	}
	printf("\n");*/

	// Generate decode matrix
	/*printf("DECODE MATRIX: \n");
	for (i = 0; i < nsrcerrs; i++){
		for(j = 0; j <k; j++ ){
			printf("%u ", decode_matrix[k*i + j]);
		}
		printf("\n");
	}
	for (p = nsrcerrs; p < nerrs; p++) { //nserrs: number of erased data unit , nerrs: Erased index
		for (i = 0; i < k; i++) {
			printf("%u ", decode_matrix[p * k + i]);
			if ((i%k) == k-1)
			printf("\n");
		}
	}
	printf("\n");*/

	// Pack recovery array as list of valid sources
	// Its order must be the same as the order
	// to generate matrix b in gf_gen_decode_matrix

	// printf("Recovery index: ");
	for (i = 0; i < k; i++)
	{
		recov[i] = buffs[decode_index[i]];
	}

	// Recover data
	ec_init_tables(k, nerrs, decode_matrix, g_tbls);									 // Generate or decode erasure codes on blocks of data
	ec_encode_data(TEST_LEN, k, nerrs, g_tbls, recov, &temp_buffs[k]); // nerrs = rows, recov = data,

	clock_t toc = clock();
	double ISAL_repair_time = (double)(toc - tic) / CLOCKS_PER_SEC;
	printf("ISA-L per erasure: %.6f seconds\n", ISAL_repair_time);
	printf("-----------------------------------------\n\n");

	// End of ISA-L repair
	//----------------------------------------------------------------

	printf("Computation time Trace/ISAL: %.1fx \n", 1.0 * trace_repair_time / ISAL_repair_time);
	printf("Computation time Trace/ISAL Accurate: %.1fx \n", 1.0 * trace_repair_time_accurate / ISAL_repair_time);
	printf("Erasure positions: ");
	for (i = 0; i < nerrs - 1; i++)
	{
		printf("%u, ", src_err_list[i]);
	}
	printf("%u\n", src_err_list[nerrs - 1]);

	return 0;
}
