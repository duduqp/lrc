//
// Created by 杜清鹏 on 2021/3/29.
//

#include <iostream>
#include "spdlog/spdlog.h"
#include "spdlog/sinks/basic_file_sink.h" // support for basic file logging
#include "erasurecoding/LRCCoder.h"


using namespace std;

auto main() -> int {
    auto logger = spdlog::basic_logger_mt("firstlogger", "log/logfile.txt", true);

    /*
     {
         //test spdlog
         logger->info("info log");

         //test LRCCoder
         auto encoder = lrc::LRCCoder({8, 2, 4});

         logger->info("lrcencoder generator matrix");

         //test data
         auto data_k_ptrs = new char *[8];
         for(int i=0;i<8;++i)
         {
             data_k_ptrs[i]=new char[32];
             std::fill(data_k_ptrs[i],data_k_ptrs[i]+32,i<3?'a': (i % 2 ? 'b' : 'd'));
         }

         for(int i = 0 ; i < 8 ;i++)
         {
             for(int j = 0;j<32;j++)
             {
                 std::cout << data_k_ptrs[i][j] <<(j<31?"\t":"\n");
             }
         }

         auto globalparity_g_ptrs = new char *[4];
         auto localparity_l_ptrs = new char*[2];

         for(int i=0;i<4;++i)
         {
             globalparity_g_ptrs[i]=new char[32];
             std::fill(globalparity_g_ptrs[i],globalparity_g_ptrs[i]+32,'1');
         }

         for(int i=0;i<2;++i)
         {
             localparity_l_ptrs[i]=new char[32];
             std::copy(data_k_ptrs[i*4],data_k_ptrs[i*4]+32,localparity_l_ptrs[i]);
         }

         for(int i =0;i<2;++i)
         {
             for(int j = 0;j<32;j++)
             {
                 std::cout << localparity_l_ptrs[i][j] <<(j<31?"\t":"\n");
             }
         }

         std::cout << "encoding generator matrix :\n";
         encoder.display_matrix();
         encoder.encode(data_k_ptrs, localparity_l_ptrs,globalparity_g_ptrs,16,32);

         //display local and global parity
         std::cout << "local parity :\n";
         for(int i =0;i<2;++i)
         {
             for(int j = 0;j<32;j++)
             {
                 std::cout << localparity_l_ptrs[i][j] <<(j<31?"\t":"\n");
             }
         }

         std::cout << "global parity :\n";
         for(int i = 0 ; i < 4 ;i++)
         {
             for(int j = 0;j<32;j++)
             {
                 std::cout << globalparity_g_ptrs[i][j] <<(j<31?"\t":"\n");
             }
         }



         //erasure 3 blocks

         std::fill(data_k_ptrs[2],data_k_ptrs[2],'0');
         std::fill(globalparity_g_ptrs[0],globalparity_g_ptrs[0],'0');
         std::fill(globalparity_g_ptrs[2],globalparity_g_ptrs[2],'0');

         int erasures[5] = {2,8,10,-1,-1};

         //prints
         encoder.decode(8,4,16,0,erasures,data_k_ptrs,globalparity_g_ptrs,32);
         std::cout << " after decode : \n" ;
         for(int i = 0 ; i < 8 ;i++)
         {
             for(int j = 0;j<32;j++)
             {
                 std::cout << data_k_ptrs[i][j] <<(j<31?"\t":"\n");
             }
         }
     }
     */



    {

    }

    return 0;
}