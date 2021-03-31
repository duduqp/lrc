//
// Created by 杜清鹏 on 2021/3/29.
//

#ifndef LRC_LRCCODER_H
#define LRC_LRCCODER_H

#include "CodeUtil.h"
#include "devcommon.h"
#include "Jerasure/include/jerasure.h"

namespace lrc{



    struct LRCCoder{
        // a light weight one shot encoder , ie : LRCCoder{}({k,l,g},const std::vector<char *>,
        // std::vector<char *> ,std::vector<char *> ,wordlength)


        //cache the matrix
        LRCCoder(std::tuple<int,int,int> stripeinfo): k(std::get<0>(stripeinfo)),
                                                      l(std::get<1>(stripeinfo)), g(std::get<2>(stripeinfo))
        {
            generator_matrix = new int[k*g];
            CodeUtil::GenerateLRCEncodingMatrix(stripeinfo,generator_matrix);
        }
        ~LRCCoder(){
            delete generator_matrix;
        }
        LRCCoder(const LRCCoder &) = delete ;
        LRCCoder(LRCCoder &&) = delete ;

        void encode(char ** data_words,
                char ** target_local_parities,char ** target_global_parities,
                int w ,int cellsize /*to set 64 KB*/){
            //do encode

            //global
            jerasure_matrix_encode(k,g,w,generator_matrix,data_words,target_global_parities,cellsize);

            //local

            std::cout << "begin xor:\n";
            for(int j=0;j<l;++j)
            {
                for(int i=1;i<g;++i)
                {
                    std::cout << " xor data block : " << j*g+i << "to :" <<"local parity block : "<<j << std::endl;
                    for(int m=0;m<l;++m)
                    {
                        for(int n=0;n<cellsize;++n)
                        {
                            std::cout << static_cast<int>(target_local_parities[m][n])<<(n<cellsize-1?"\t":"\n");
                        }
                    }
                    galois_region_xor(data_words[j*g+i],target_local_parities[j],cellsize);
                }
            }


            //debug
            std::cout << "debug:\n";
            for(int i=0;i<l;++i)
            {
                for(int j=0;j<cellsize;++j)
                {
                    std::cout << static_cast<int>(target_local_parities[i][j])<<(j<cellsize-1?"\t":"\n");
                }
            }
        }

        void decode(int p_k, int p_m, int w,
                    int row_k_ones, int *erasures,
                    char **data_ptrs, char **coding_ptrs, int cellsize)
        {
            jerasure_matrix_decode(8,4,16,generator_matrix,0,erasures,data_ptrs,coding_ptrs,cellsize);
        }
        void display_matrix()
        {
            if(nullptr == generator_matrix)
            {
                return ;
            }
            jerasure_print_matrix(generator_matrix,g,k,16);
        }


    private:
        int * generator_matrix; // default non-defined value
        int k ;
        int l ;
        int g ;

    };
}


#endif //LRC_LRCCODER_H
