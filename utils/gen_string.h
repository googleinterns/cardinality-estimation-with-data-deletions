/*
 * random string stream generator:
 * generate a stream of string randomly with the length of the string between [len_min, len_max]
 * Example: generating a string stream with 10 strings.
 *   gen_string gen;
 *   for (int i=0; i<10; i++) gen.next();
 */

#ifndef GEN_STRING_
#define GEN_STRING_ 

#include <iostream>
#include <string>
#include <random>

class gen_string {

  public:
    /*
     * default constructor 
     * @seed=1 
     * @min_len=6 
     * @max_len=20
     * @pool={'0',...,'9','A',...,'Z','a',...,'z'}
     */
    gen_string():seed(1),min_len(6),max_len(20) {
      gen.seed(seed);
      generate_pool();
      dist_code=std::uniform_int_distribution<int>(0,pool.size()-1);
      dist_len=std::uniform_int_distribution<int>(min_len,max_len);
    }

    /*
     * constructor with @seed, @min_len and @max_len given 
     * @seed=seed0
     * @min_len=min_len0
     * @max_len=max_len0
     * @pool is set to be {'0',...,'9','A',...,'Z','a',...,'z'}
     */
    gen_string(const int& seed0, const int& min_len0, const int& max_len0): seed(seed0), min_len(min_len0), max_len(max_len0){
      gen.seed(seed);
      generate_pool();
      dist_code=std::uniform_int_distribution<int>(0,pool.size()-1);
      dist_len=std::uniform_int_distribution<int>(min_len,max_len);
    }

    /*
     * constructor with @seed, @min_len, @max_len and @pool given 
     * @seed=seed0
     * @min_len=min_len0
     * @max_len=max_len0
     * @pool=pool0
     */
    gen_string(const int& seed0, const int& min_len0, const int& max_len0, const std::vector<char>& pool0): seed(seed0), min_len(min_len0), max_len(max_len0), pool(pool0){
      gen.seed(seed);
      dist_code=std::uniform_int_distribution<int>(0,pool.size()-1);
      dist_len=std::uniform_int_distribution<int>(min_len,max_len);
    }

    // Returns the next random string of the string stream
    std::string next() {
      std::string ret;
      int len=dist_len(gen);
      for (int l=0; l<len; l++) ret.push_back(pool[dist_code(gen)]); 
      return ret;
    }

  private:
    // random generator
    std::mt19937 gen;
    // character generator, when it's called as dist_code(gen), an index of the pool will be generated
    std::uniform_int_distribution<int> dist_code;
    // target length of the next string generator, when it's called as dist_len(gen), the target length is generated
    std::uniform_int_distribution<int> dist_len;

    // @seed seed of the random generator
    int seed;
    // @min_len lower bound of the length of the random generated string
    // @max_len upper bound of the length of the random generated string
    int min_len, max_len;
    // @pool a pool of characters to draw when generating a random string
    std::vector<char> pool;

    // generate the default pool
    // @pool is set to be {'0',...,'9','A',...,'Z','a',...,'z'}
    void generate_pool() {
      for (char ch='0'; ch<='9'; ch++) pool.push_back(ch);
      for (char ch='A'; ch<='Z'; ch++) pool.push_back(ch);
      for (char ch='a'; ch<='z'; ch++) pool.push_back(ch);
    }

};

#endif
