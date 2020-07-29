#ifndef GEN_DATA_STREAM_
#define GEN_DATA_STREAM_

#include <random>
#include <limits>
#include <string>
#include <map>
#include <vector>

namespace datasketches {
class GenDataStream {
 public:
  enum OP { ADD = 0, DELETE = 1 };
  GenDataStream() : GenDataStream(0, 10000) {}
  GenDataStream(double delete_percentage0, int pool_size0)
      : GenDataStream(delete_percentage0, pool_size0, 1) {}

  GenDataStream(double delete_percentage0, int pool_size0, int seed0)
      : delete_percentage(delete_percentage0),
        pool_size(pool_size0),
        seed(seed0) {
    initializeRandom();
  }

  std::pair<OP, int> next() {
    if (d_op(gen) && all_elements.size() != 0) {  // delete
      int idx = d_del_idx(gen) % all_elements.size();
      int x = all_elements[idx];
      remove(idx);
      return std::make_pair(DELETE, x);
    } else {  // add
      int x = d_data(gen);
      add(x);
      return std::make_pair(ADD, x);
    }
  }

  std::vector<int> batch(OP op, int num) {
    std::vector<int> data_stream;
    if (op==ADD) {
      for (int i = 0; i < num; i++) {
        int x = d_data(gen);
        add(x);
        data_stream.push_back(x);
      }
    } else {
      for (int i = 0; i < num; i++) {
        if (static_cast<int>(all_elements.size()) >= 0) {
          int idx = d_del_idx(gen) % all_elements.size();
          int x = all_elements[idx];
          remove(idx);
          data_stream.push_back(x);
        }
      } 
      /* else */
      /*   throw std::logic_error( */
      /*       "Number of existing elements is smaller than required number of " */
      /*       "deletions!"); */
    }
    return data_stream;
  }

  int getNumDistinctElements() { return current_distinct_elements.size(); }

 private:
  // @delete_percentage number of delete operations / number of addition
  // operations
  double delete_percentage;
  // @pool_size the size of the pool of all elements
  int pool_size;
  // @seed seed for the random generator
  int seed;

  // @current_distinct_elements stores the count for each elements
  std::map<int, uint64_t> current_distinct_elements;
  // @all_elements record all existing elements with duplications 
  std::vector<int> all_elements;
   
  std::random_device rd{};
  // generator of the input data
  std::mt19937 gen{rd()};
  // distribution of the input data
  std::uniform_int_distribution<> d_data;
  // bernoulli_distribution: determine the operation at this step (insert / delete)
  std::bernoulli_distribution d_op;
  // the index in the already generated data to be deleted
  std::uniform_int_distribution<> d_del_idx;

  void add(int x) {
    all_elements.push_back(x);
    current_distinct_elements[x]++;
  }

  void remove(int idx) {
    std::swap(all_elements[idx], all_elements.back());
    int x = all_elements.back();
    all_elements.pop_back();
    if (--current_distinct_elements[x] == 0)
      current_distinct_elements.erase(x);
  }

  void initializeRandom() {
    gen.seed(seed);
    d_data = std::uniform_int_distribution<>(1, pool_size);
    d_op = std::bernoulli_distribution(delete_percentage /
                                       (1 + delete_percentage));
    d_del_idx = std::uniform_int_distribution<>(0, std::numeric_limits<int>::max());
  }

};

}/* namespace datasketches */

#endif
