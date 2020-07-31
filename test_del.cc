#include <gtest/gtest.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <set>
#include <map>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include "gen_string.h"
#include "gen_data_stream.h"
#include "theta_sketch_dup.h"

using namespace datasketches;
using namespace std;

const int TOT_OPERATIONS = 1<<20;

int main() {
  
  std::string filename="data";
  fstream output(filename, ios::out | ios::trunc | ios::binary);

  for (int s=1; s<=100; s++) {
    GenDataStream data_gen(0, 100000000, s);
    auto a = update_theta_sketch_dup::builder().set_lg_k(12).build();
    vector<int> v_add = data_gen.batch(GenDataStream::OP::ADD, TOT_OPERATIONS);
    for (auto x : v_add) a.update(x);
    for (int prev_num = TOT_OPERATIONS, num_op = TOT_OPERATIONS >> 1;
         num_op > 0; prev_num = num_op, num_op >>= 1) {
      vector<int> v_del = data_gen.batch(GenDataStream::OP::DELETE, prev_num-num_op);
      for (auto x : v_del) a.remove(x);
      output << a.get_estimate() / data_gen.getNumDistinctElements() << " ";
    }
    output << endl;
  }

  // for (int s=1; s<=100; s++) {
  //   GenDataStream data_gen(0.999, 100000, s);
  //   auto a = update_theta_sketch_dup::builder().set_lg_k(12).build();
  //   for (int i = 0; i < TOT_OPERATIONS; i++) {
  //     auto d = data_gen.next();
  //     // cout << (d.first==0?"add":"delete") << " " << d.second << endl;
  //     if (d.first == 0)
  //       a.update(d.second);
  //     else
  //       a.remove(d.second);
  //   }
  //   output << a.get_estimate() / data_gen.getNumDistinctElements() << endl;
  // }

  // cout << "number of distinct elements: " << data_gen.getNumDistinctElements() << endl;
  // cout << "estimation of the sketch:    " << a.get_estimate() << endl;
  // cout << a.to_string();

  return 0;
}
