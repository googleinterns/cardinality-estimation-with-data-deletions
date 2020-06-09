#ifndef GEN_STRING_
#define GEN_STRING_ 

#include <iostream>
#include <string>
#include <random>

class gen_string {

  public:

    std::string next() {
      std::string ret;
      int len=dist_len(gen);
      for (int l=0; l<len; l++) ret.push_back(pool[dist_code(gen)]); 
      return ret;
    }

    gen_string() {
      gen.seed(1);
      generate_pool();
      dist_code=std::uniform_int_distribution<int>(0,pool.size()-1);
      dist_len=std::uniform_int_distribution<int>(6,20);
    }

  private:
    std::mt19937 gen;
    std::uniform_int_distribution<int> dist_code;
    std::uniform_int_distribution<int> dist_len;
    std::vector<char> pool;

    void generate_pool() {
      for (char ch='0'; ch<='9'; ch++) pool.push_back(ch);
      for (char ch='A'; ch<='Z'; ch++) pool.push_back(ch);
      for (char ch='a'; ch<='z'; ch++) pool.push_back(ch);
    }

};

#endif
