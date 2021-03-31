//
// Created by 杜清鹏 on 2021/3/29.
//

#ifndef LRC_CODEUTIL_H
#define LRC_CODEUTIL_H

#include <cassert>
#include "Jerasure/include/galois.h"
#include "devcommon.h"

namespace lrc{

    namespace CodeUtil{
        void GenerateLRCEncodingMatrix(std::tuple<int,int,int> stripeinfo ,
                int * matrix){
            // param : in:{k,l,g} matrix[g*k]
            int k = std::get<0>(stripeinfo);
            int l = std::get<1>(stripeinfo);
            int g = std::get<2>(stripeinfo);
            if(g>=32){
                assert("g must smaller than 32");
                return ;
            }
            int r = g ; //  assume g = k/l
            int width =l * ceil(log2(r+1));
            int w = l * width;

            // start at 1 , consider first local group
            int cursor = 0;
            for(int i=0;i<r;++i)
            {
                matrix[0*k+cursor] = (i + 1);
                cursor++;
            }

            for(int j = 1;j<l;++j)
            {
                for(int i=0;i<r;++i)
                {
                    matrix[0*k+cursor] = matrix[(0*k+cursor)%r]<<(j*width);
                    cursor++;
                }
            }

            //next row
            for(int j = 1;j < g;++j)
            {
                for(int i = 0;i<k;++i)
                    matrix[j*k+i]=galois_single_multiply(matrix[(j-1)*k+i],matrix[(j-1)*k+i],16);
            }

        }
    }
}

#endif //LRC_CODEUTIL_H
