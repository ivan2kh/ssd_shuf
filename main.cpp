#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <vector>
#include <type_traits>
#include <stdint.h>
#include <limits.h>
#include <algorithm>
#include <stdio.h>
#include <sys/resource.h>
#include <random>

#include "Bucket.h"

using namespace std;

void display(
        std::string name,
        std::chrono::time_point<std::chrono::high_resolution_clock> start,
        std::chrono::time_point<std::chrono::high_resolution_clock> end
) {
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    cerr << name << " " << elapsed << " ms" << "\n";
}

struct FasterSort {
    FasterSort(const char* fileName) {
        f = fopen(fileName, "rb");
        if (!f) {
            throw runtime_error("cannot open input file");
        }
    }

    void shuf() {
        const size_t bucketSize = 1024*10;
        const size_t bucketsCount = 1024;

        std::random_device rd;  //Will be used to obtain a seed for the random number engine
        std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
        std::uniform_int_distribution<> dis(0, bucketsCount-1);

        vector<Bucket<bucketSize> > buckets(bucketsCount);

        auto start = std::chrono::high_resolution_clock::now();

        const size_t bufSize = 1024*1024;
        std::vector<uint8_t> buf(bufSize);

        size_t begin = 0; //TODO: replace with cyclic buffer

        size_t appends =0;
        while(!feof(f)) { // TODO: fix bug with lines longer than bufSize
            size_t read = std::fread(&buf[begin], 1, bufSize - begin, f);
            size_t end = begin+read;

            //Calculate lines offsets
            size_t offset = 0;
            size_t prevOffset = 0;
            while (uint8_t *newLinePos = (uint8_t *) memchr(&buf[offset], '\n', end - offset)) {
                prevOffset = offset;
                offset = newLinePos - &buf[0] + 1;
                int bucket = dis(gen);
                buckets[bucket].append(&buf[prevOffset], offset-prevOffset-1);
                appends += offset-prevOffset;
            }

            //fix last line
            if(offset != end) {
                memcpy(&buf[0], &buf[offset], bufSize - end);
            }
            begin = end - offset;
        }
        if(begin) {
            int bucket = dis(gen);
            buckets[bucket].append(&buf[0], begin);
            appends += begin + 1;
        }
        cerr << "appends " << appends<<"\n";

        for(auto& bucket: buckets) {
            bucket.stopFill();
        }

        size_t tot = 0;
        for(auto& bucket: buckets) {
            tot += bucket.shuffleAndWrite(stdout);
        }
        cerr<<"bytes written " << tot<<"\n";

        auto end = std::chrono::high_resolution_clock::now();
        display("tbb shuf", start, end);
    }

    ~FasterSort() {
        if(f) {
            fclose(f);
        }
    }

    FILE* f = nullptr;
    vector<uint8_t> buf{0};
};

int main(int argc, char* argv[]) {
    rlimit l{0};
    getrlimit(RLIMIT_NOFILE, &l);
    l.rlim_cur = 10+1024*2;
    if (setrlimit(RLIMIT_NOFILE, &l))
        throw runtime_error("cannot increase RLIMIT_NOFILE");

    if(argc!=2) {
        cerr<<"Write a random permutation of the input lines to standard output\n"
            <<"Usage:\n\t"
            <<argv[0]<<" <input_file>\n\n"
            <<"The result will be printed to standard output\n";
        return -1;
    }
    try {
        FasterSort sort(argv[1]);
        sort.shuf();
    } catch (const exception& ex) {
        cerr << ex.what() << "\n";
        return -2;
    }
    return 0;
}