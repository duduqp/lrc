//
// Created by 杜清鹏 on 2021/3/29.
//

#ifndef LRC_METAINFO_H
#define LRC_METAINFO_H
#include "devcommon.h"

namespace lrc{
    /*
     * this file provides general filesystem meta infomation such as StripeInfo , ClusterInfo etc.
    */

    struct ECSchema{
        int datablk;
        int localparityblk;
        int globalparityblk;
        int blksize; // count Bytes
    };
    enum ErasureCodingPolicy{
        LRC = 0 // default
    };
    struct StripeInfo{
        /*
         * describe a stripe[same as a file in this project] , consists of stripe width , local parity
         * and global parity blocks , currently our own LRC ECSchema
        */

        //location : an uri vector
        //ie : [{datablk :}ip:port1,ip:port2,...|{local_parity_blk :}ip:port k+1,...|{global_parityblk : }... ]
        std::vector<std::string> blklocation;

        //ie : [cluster1 , cluster1 ,cluster2,] means blklocation[i] coming from clusterdistribution[i]
        std::vector<int> clusterdistribution;

        int stripeid;
        ECSchema ecschema;
        ErasureCodingPolicy ecpolicy;
    };


    /*
     * decribe a filesystem namespace
     */
    /*
    struct FileSystemDirectoryTree{
        struct FileSystemDirectoryNode{
            FileSystemObject
            std::vector<FileSystemDirectoryNode *> childnodes;
        };
    };
    */
}

#endif //LRC_METAINFO_H
