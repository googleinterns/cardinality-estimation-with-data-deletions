#include <gtest/gtest.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <set>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include "gen_string.h"
#include "theta_sketch_dup.h"

using namespace datasketches;
using namespace std;

const int STREAM_LENGTH = 2500000;

int main() {
  
  std::string filename="data";
  std::fstream output(filename, ios::out | ios::trunc | ios::binary);
  for (int i=10; i<20; i++) {
    gen_string gen;
    for (int j=0; j<1; j++)
    {
    auto a = update_theta_sketch_dup::builder().set_lg_k(i).build();
    for (int j = 0; j < STREAM_LENGTH; j++) a.update(gen.next());
    datasketches_pb::ThetaSketchDup serialized_proto;
    a.serialize(&serialized_proto);
    datasketches_pb::ThetaSketchDup tmp(serialized_proto);
    tmp.clear_keys();
    auto* hash_pair = tmp.add_keys();
    hash_pair->set_hash_val(serialized_proto.keys(0).hash_val());
    hash_pair->set_count(serialized_proto.keys(0).count());
    for (unsigned int i = 1; i < serialized_proto.num_keys(); i++) {
      auto* hash_pair = tmp.add_keys();
      hash_pair->set_hash_val(serialized_proto.keys(i).hash_val()-serialized_proto.keys(i-1).hash_val());
      hash_pair->set_count(serialized_proto.keys(i).count());
    }
    output << std::setw(10) << serialized_proto.ByteSize() << std::setw(20) << std::setprecision(9) << 2/sqrt(a.get_num_retained()) << std::endl;
    // datasketches_pb::ThetaSketchDup tmp(serialized_proto);
    // tmp.clear_keys();
    // auto* hash_pair = tmp.add_keys();
    // hash_pair->set_hash_val(serialized_proto.keys(0).hash_val());
    // hash_pair->set_count(serialized_proto.keys(0).count());
    // for (unsigned int i = 1; i < serialized_proto.num_keys(); i++) {
    //   tmp.set_num_keys(i);
    //   tmp.set_theta(serialized_proto.keys(i).hash_val());
    //   if (i > 1000 && i % 100==0)
    //     output << std::setw(10) << tmp.ByteSize() << std::setw(20)
    //            << std::setprecision(9) << 2 / sqrt(tmp.num_keys()) << std::endl;
    //   auto* hash_pair = tmp.add_keys();
    //   hash_pair->set_hash_val(serialized_proto.keys(i).hash_val()-serialized_proto.keys(i-1).hash_val());
    //   hash_pair->set_count(serialized_proto.keys(i).count());
    // }
    // std::string filename="data" +to_string(i);
    // std::fstream output(filename, ios::out | ios::trunc | ios::binary);
    // serialized_proto.SerializeToOstream(&output);
    // std::fstream input(filename, ios::in | ios::binary);
    // serialized_proto.ParseFromIstream(&input);
    // auto b = update_theta_sketch_dup::deserialize(serialized_proto);
  }
  }
  // for (int lg_k = 7; lg_k <= 7; lg_k++) {
  //     std::vector<double> estimations;
  //     gen_string gen;
  //     for (int j = 0; j < 100; j++) {
  //       auto a = update_theta_sketch_dup::builder().set_lg_k(lg_k).build();
  //       for (int i = 0; i < STREAM_LENGTH; i++) a.update(gen.next());
  //       estimations.push_back(a.get_estimate());
  //     }
  //     std::ofstream output("test_data/2_to_27/lgk=" + std::to_string(lg_k),
  //                          std::ofstream::out);
  //     for (auto e : estimations) output << e << std::endl;
  //     output.close();
  // }
  // auto serialized_bytes = a.serialize();
  // std::ofstream output("data",std::ofstream::out);
  // for (auto e:serialized_bytes) {
  //   output << e << " ";
  // }
  // std::cout << "------------------------------------------------------" <<
  // std::endl; std::cout << "Estimation of the cardinality is: " <<
  // a.get_estimate() << std::endl;
  // std::cout << "------------------------------------------------------" <<
  // std::endl;
  return 0;
}
