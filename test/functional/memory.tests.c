#include "ccv.h"
#include "ccv_internal.h"
#include "case.h"

#include <sys/time.h>
#include <dispatch/dispatch.h>

static unsigned int get_current_time()
{
	struct timeval tv;
	gettimeofday(&tv, 0);
	return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

uint64_t uniqid()
{
	union {
		uint64_t u;
		uint8_t chr[8];
	} sign;
	int i;
	for (i = 0; i < 8; i++)
		sign.chr[i] = rand() & 0xff;
	return sign.u;
}

#define N (1000000)

TEST_CASE("synchronous random cache put/delete/get")
{
    unsigned int start_time = get_current_time();
	ccv_cache_t cache;
	ccv_cache_init(&cache, N, 1, ccfree);
	uint64_t* sigs = ccmalloc(sizeof(uint64_t) * N);
	void** mems = ccmalloc(sizeof(void*) * N);
	int i;
	for (i = 0; i < N; i++)
	{
		sigs[i] = uniqid();
		mems[i] = ccmalloc(1);
		ccv_cache_put(&cache, sigs[i], mems[i], 1, 0);
	 	REQUIRE_EQ(i + 1, cache.size, "at %d should has cache size %d", i, i);
	}
	uint8_t deleted[N];
	for (i = 0; i < N; i++)
	{
		deleted[i] = 1;
		if (deleted[i])
			ccv_cache_delete(&cache, sigs[i]);
	 	REQUIRE_EQ(N - 1 - i, cache.size, "at %d should has cache size %d", i, N - 1 - i);
	}
	for (i = 0; i < N; i++)
	{
		deleted[i] = (rand() % 3 == 0);
		if (!deleted[i])
		{
			mems[i] = ccmalloc(1);
			ccv_cache_put(&cache, sigs[i], mems[i], 1, 0);
		}
	}
	for (i = 0; i < N; i++)
	{
		deleted[i] = (rand() % 3 == 0);
		if (deleted[i])
			ccv_cache_delete(&cache, sigs[i]);
		else {
			mems[i] = ccmalloc(1);
			ccv_cache_put(&cache, sigs[i], mems[i], 1, 0);
		}
	}
	for (i = 0; i < N; i++)
	{
		void* x = ccv_cache_get(&cache, sigs[i], 0);
		if (!deleted[i] && x) // x may be pull off the cache
		{
			REQUIRE_EQ((uint64_t)mems[i], (uint64_t)x, "value at %d should be consistent", i);
		} else
	 		REQUIRE_EQ(0, (uint64_t)x, "at %d should not exist", i);
	}
	ccv_cache_close(&cache);
    unsigned int end_time = get_current_time();
    printf("total time: %ums\n", (end_time-start_time));
	ccfree(mems);
	ccfree(sigs);
}

TEST_CASE("synchronous garbage collector 95\% hit rate")
{
    unsigned int start_time = get_current_time();
	int i;
	// deliberately let only cache size fits 90% of data
	ccv_enable_cache((sizeof(ccv_dense_matrix_t) + 4) * N * 90 / 100);
	for (i = 0; i < N; i++)
	{
		ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, 0);
		dmt->data.i32[0] = i;
		dmt->sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
		dmt->type |= CCV_REUSABLE;
		ccv_matrix_free(dmt);
	}
	int percent = 0, total = 0;
	for (i = N - 1; i > N * 6 / 100; i--)
	{
		uint64_t sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
		ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, sig);
		if (i == dmt->data.i32[0])
			++percent;
		++total;
		ccv_matrix_free_immediately(dmt);
	}
	REQUIRE((double)percent / (double)total > 0.95, "the cache hit (%lf) should be greater than 95%%", (double)percent / (double)total);
	ccv_disable_cache();
    unsigned int end_time = get_current_time();
    printf("total time: %ums\n", (end_time-start_time));
}

TEST_CASE("synchronous garbage collector 47\% hit rate")
{
    unsigned int start_time = get_current_time();
	int i;
	// deliberately let only cache size fits 90% of data
	ccv_enable_cache((sizeof(ccv_dense_matrix_t) + 4) * N * 45 / 100);
	for (i = 0; i < N; i++)
	{
		ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, 0);
		dmt->data.i32[0] = i;
		dmt->sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
		dmt->type |= CCV_REUSABLE;
		ccv_matrix_free(dmt);
	}
	int percent = 0, total = 0;
	for (i = N - 1; i > N * 6 / 100; i--)
	{
		uint64_t sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
		ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, sig);
		if (i == dmt->data.i32[0])
			++percent;
		++total;
		ccv_matrix_free_immediately(dmt);
	}
	REQUIRE((double)percent / (double)total > 0.47, "the cache hit (%lf) should be greater than 47%%", (double)percent / (double)total);
	ccv_disable_cache();
    unsigned int end_time = get_current_time();
    printf("total time: %ums\n", (end_time-start_time));
}

TEST_CASE("synchronous multi-type garbage collector 92\% hit rate")
{
    unsigned int start_time = get_current_time();
	int i;
	ccv_enable_cache(((sizeof(ccv_dense_matrix_t) + 4) + (sizeof(ccv_array_t) + 4 * 4)) * N * 90 / 100);
	for (i = 0; i < N; i++)
	{
		ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, 0);
		dmt->data.i32[0] = i;
		dmt->sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
		dmt->type |= CCV_REUSABLE;
		ccv_matrix_free(dmt);
		ccv_array_t* array = ccv_array_new(4, 4, 0);
		int j = i;
		ccv_array_push(array, &j);
		j = 0;
		ccv_array_push(array, &j);
		j = ~i;
		ccv_array_push(array, &j);
		j = -i;
		ccv_array_push(array, &j);
		array->sig = ccv_cache_generate_signature((const char*)&j, 4, CCV_EOF_SIGN);
		array->type |= CCV_REUSABLE;
		ccv_array_free(array);
	}
	int percent = 0, total = 0;
	for (i = N - 1; i > N * 6 / 100; i--)
	{
		uint64_t sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
		ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, sig);
		if (i == dmt->data.i32[0])
			++percent;
		++total;
		ccv_matrix_free_immediately(dmt);
		int j = -i;
		sig = ccv_cache_generate_signature((const char*)&j, 4, CCV_EOF_SIGN);
		ccv_array_t* array = ccv_array_new(4, 4, sig);
		if (i == *(int*)ccv_array_get(array, 0) &&
			0 == *(int*)ccv_array_get(array, 1) &&
			~i == *(int*)ccv_array_get(array, 2) &&
			-i == *(int*)ccv_array_get(array, 3))
			++percent;
		++total;
		ccv_array_free_immediately(array);
	}
	REQUIRE((double)percent / (double)total > 0.92, "the cache hit (%lf) should be greater than 92%%", (double)percent / (double)total);
	ccv_disable_cache();
    unsigned int end_time = get_current_time();
    printf("total time: %ums\n", (end_time-start_time));
}

TEST_CASE("concurrent random cache put/delete/get")
{
    unsigned int start_time = get_current_time();
	__block ccv_cache_t cache = { NULL };
	ccv_cache_init(&cache, N, 1, ccfree);
	uint64_t* sigs = ccmalloc(sizeof(uint64_t) * N);
	void** mems = ccmalloc(sizeof(void*) * N);
	uint8_t* deleted = ccmalloc(sizeof(uint8_t) * N);

	// rand() is not thread safe so we will generate
	// all the unique signatures up front.
	for (int i = 0; i < N; i++) {
		sigs[i] = uniqid();
	}
	
	dispatch_queue_t global_queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
	size_t iterations = N;
    dispatch_apply(iterations, global_queue, ^(size_t i) {
		mems[i] = ccmalloc(1);
		ccv_cache_put(&cache, sigs[i], mems[i], 1, 0);
    });	
 	REQUIRE_EQ(N, cache.size, "should have cache size %d", N);
	
    dispatch_apply(iterations, global_queue, ^(size_t i) {
		deleted[i] = 1;
		if (deleted[i]) {
			ccv_cache_delete(&cache, sigs[i]);			
		}
	});
	REQUIRE_EQ(0, cache.size, "should have cache size %d", 0);
	
	
    dispatch_apply(iterations, global_queue, ^(size_t i) {
		deleted[i] = (rand() % 3 == 0);
		if (!deleted[i])
		{
			mems[i] = ccmalloc(1);
			ccv_cache_put(&cache, sigs[i], mems[i], 1, 0);
		}
	});
	
    dispatch_apply(iterations, global_queue, ^(size_t i) {
		deleted[i] = (rand() % 3 == 0);
		if (deleted[i]) {
			ccv_cache_delete(&cache, sigs[i]);			
		} else {
			mems[i] = ccmalloc(1);
			ccv_cache_put(&cache, sigs[i], mems[i], 1, 0);
		}
	});
	
    dispatch_apply(iterations, global_queue, ^(size_t i) {
		void* x = ccv_cache_get(&cache, sigs[i], 0);
		if (!deleted[i] && x) // x may be pull off the cache
		{
			REQUIRE_EQ((uint64_t)mems[i], (uint64_t)x, "value at %zd should be consistent", i);
		} else
	 		REQUIRE_EQ(0, (uint64_t)x, "at %zd should not exist", i);
	});
	ccv_cache_close(&cache);
    unsigned int end_time = get_current_time();
    printf("total time: %ums\n", (end_time-start_time));
	ccfree(mems);
	ccfree(sigs);
	ccfree(deleted);
}

TEST_CASE("concurrent garbage collector 95\% hit rate")
{
	dispatch_queue_t global_queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
	size_t iterations = N;	
    unsigned int start_time = get_current_time();
	// deliberately let only cache size fits 90% of data
	ccv_enable_cache((sizeof(ccv_dense_matrix_t) + 4) * N * 90 / 100);
    dispatch_apply(iterations, global_queue, ^(size_t i) {
		ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, 0);
		dmt->data.i32[0] = i;
		dmt->sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
		dmt->type |= CCV_REUSABLE;
		ccv_matrix_free(dmt);
	});
	__block int percent = 0, total = 0;
    dispatch_apply(iterations, global_queue, ^(size_t i) {	
		if (i > N * 6 / 100) {
			uint64_t sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
			ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, sig);
			if (i == dmt->data.i32[0]) {
				++percent;				
			}
			++total;
			ccv_matrix_free_immediately(dmt);
		}
	});
	REQUIRE((double)percent / (double)total > 0.95, "the cache hit (%lf) should be greater than 95%%", (double)percent / (double)total);
	ccv_disable_cache();
    unsigned int end_time = get_current_time();
    printf("total time: %ums\n", (end_time-start_time));
}

TEST_CASE("concurrent garbage collector 47\% hit rate")
{
	dispatch_queue_t global_queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
	size_t iterations = N;
    unsigned int start_time = get_current_time();
	// deliberately let only cache size fits 90% of data
	ccv_enable_cache((sizeof(ccv_dense_matrix_t) + 4) * N * 45 / 100);
    dispatch_apply(iterations, global_queue, ^(size_t i) {
		ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, 0);
		dmt->data.i32[0] = i;
		dmt->sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
		dmt->type |= CCV_REUSABLE;
		ccv_matrix_free(dmt);
	});
	__block int percent = 0, total = 0;
    dispatch_apply(iterations, global_queue, ^(size_t i) {	
		if (i > N * 6 / 100) {
			uint64_t sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
			ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, sig);
			if (i == dmt->data.i32[0]) {
				++percent;				
			}
			++total;
			ccv_matrix_free_immediately(dmt);
		}
	});
	REQUIRE((double)percent / (double)total > 0.47, "the cache hit (%lf) should be greater than 47%%", (double)percent / (double)total);
	ccv_disable_cache();
    unsigned int end_time = get_current_time();
    printf("total time: %ums\n", (end_time-start_time));
}

TEST_CASE("concurrent multi-type garbage collector 92\% hit rate")
{
	dispatch_queue_t global_queue = dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0);
	size_t iterations = N;
    unsigned int start_time = get_current_time();
	ccv_enable_cache(((sizeof(ccv_dense_matrix_t) + 4) + (sizeof(ccv_array_t) + 4 * 4)) * N * 90 / 100);
    dispatch_apply(iterations, global_queue, ^(size_t i) {
		ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, 0);
		dmt->data.i32[0] = i;
		dmt->sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
		dmt->type |= CCV_REUSABLE;
		ccv_matrix_free(dmt);
		ccv_array_t* array = ccv_array_new(4, 4, 0);
		int j = i;
		ccv_array_push(array, &j);
		j = 0;
		ccv_array_push(array, &j);
		j = ~i;
		ccv_array_push(array, &j);
		j = -i;
		ccv_array_push(array, &j);
		array->sig = ccv_cache_generate_signature((const char*)&j, 4, CCV_EOF_SIGN);
		array->type |= CCV_REUSABLE;
		ccv_array_free(array);
	});
	__block int percent = 0, total = 0;
    dispatch_apply(iterations, global_queue, ^(size_t i) {	
		if (i > N * 6 / 100) {
			uint64_t sig = ccv_cache_generate_signature((const char*)&i, 4, CCV_EOF_SIGN);
			ccv_dense_matrix_t* dmt = ccv_dense_matrix_new(1, 1, CCV_32S | CCV_C1, 0, sig);
			if (i == dmt->data.i32[0])
				++percent;
			++total;
			ccv_matrix_free_immediately(dmt);
			int j = -i;
			sig = ccv_cache_generate_signature((const char*)&j, 4, CCV_EOF_SIGN);
			ccv_array_t* array = ccv_array_new(4, 4, sig);
			if (i == *(int*)ccv_array_get(array, 0) &&
				0 == *(int*)ccv_array_get(array, 1) &&
				~i == *(int*)ccv_array_get(array, 2) &&
				-i == *(int*)ccv_array_get(array, 3))
				++percent;
			++total;
			ccv_array_free_immediately(array);
		}
	});
	REQUIRE((double)percent / (double)total > 0.92, "the cache hit (%lf) should be greater than 92%%", (double)percent / (double)total);
	ccv_disable_cache();
    unsigned int end_time = get_current_time();
    printf("total time: %ums\n", (end_time-start_time));
}

#include "case_main.h"
