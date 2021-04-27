# -*- coding: utf-8 -*-
"""
Created on Sat Apr 25 18:40:47 2020

@author: mmata
"""

from pathlib import Path
import os.path
import os
import numpy as np
import re
import pandas as pd
import shutil 

from multiprocessing import Pool
import multiprocessing
from itertools import repeat
import sys
sys.setrecursionlimit(20000)




csv_SIG = 'D:/mm_thesis/Software_graphs/SIG_graph/'
csv_path_l = os.listdir(csv_SIG)
csv_files = [(os.path.join(csv_SIG,infile)+"/") for infile in csv_path_l]

def chunks(l, n):
   return [l[i:i+n] for i in range(0, len(l), n)]

def get_new(edges_df, node_df):
    for i, row in edges_df.iterrows():
                #print(row)
        s_i =node_df[node_df['Node']==row['source']]['Node_id'].values[0]
        t_i =node_df[node_df['Node']==row['target']]['Node_id'].values[0]
               
                #print('s_t',s_i, t_i)
        edges_df.at[i,"t_id"] = t_i
        edges_df.at[i,"s_id"] = s_i
    #edges_df.rename(columns= {'source':'i_source', 'target' :'i_target', 's_id': 'O_source', 't_id' : 'O_target'}, inplace = True)
            
           
  
    return edges_df
            

            
            
            
if __name__ == '__main__':          
 

    csv_SIG = 'D:/mm_thesis/Software_graphs/SIG_graph/'
    csv_path_l = os.listdir(csv_SIG)
    csv_files = [(os.path.join(csv_SIG,infile)+"/") for infile in csv_path_l]
    #lookup_file1 = "C:/Users/mmata/Desktop/Edgelists/tstamp_lookup.csv"
    #lookup1_df =  pd.read_csv(lookup_file1)
#  =======================================================
    for net in csv_files:

        print('net:    ',net)
        net_list = os.listdir(net)
    #     print(net_list)
    
        nodes_files = [(os.path.join(net,infile ))for infile in net_list if os.path.isfile(os.path.join(net,infile))]
        net_dir = [(os.path.join(net,infile)+'/')for infile in net_list if os.path.isdir(os.path.join(net,infile))] 
    #     print('net_dir',net_dir)
        nodes_nodup = [j for j in nodes_files if j.find('nodup')!=-1 ]
        print('all_nodes', nodes_nodup)
        nodes_all = nodes_nodup[0]
        node_df = pd.read_csv(nodes_all)
     
        for g in net_dir:    
            print(g)
            g_list = os.listdir(g)
            edges_files = [(os.path.join(g,i)+'/') for i in g_list if ((i.find('Edges') != -1) and (os.path.isdir(os.path.join(g,i))))]
    # # 
            print('edged>>>>>>>>>>>>>>>>>',edges_files)
            for net_graph in edges_files:
                print('all_nodes',nodes_all)
                nn_list = os.listdir(net_graph)
                for n in nn_list:
                    print(50 * '+')
                    print(net_graph + n)
                    e_file = os.path.join(net_graph,n)
                    print('edge_file', e_file)
                    edges_df = pd.read_csv(e_file)
                    edges_df= edges_df.assign(s_id =" ")
                    edges_df= edges_df.assign(t_id =" ")
                
                    print("df_size", edges_df.shape)
                    edges_chunks = chunks(edges_df,1000)
                    p = Pool(8)
                    print("getting node id")
                    data_l = []
                    chunk_df = pd.DataFrame() 
                    m_df = pd.DataFrame()
                    for  i in range(len(edges_chunks)):
                   #for item in edges_chunks:
                   
                        M = p.starmap(get_new, zip([edges_chunks[i]], repeat(node_df)))
                  
                        m_df = pd.concat(M)
                    #print('m_df', m_df.shape)
                        chunk_df = pd.concat([chunk_df,m_df])
                        print(chunk_df.shape)
                #print(chunk_df.head())
               
                    chunk_df.rename(columns= {'source':'i_source', 'target' :'i_target',
                                          's_id': 'source', 't_id' : 'target'}, inplace = True)
                #chunk_df.rename(columns= {'source':'o_source', 'target' :'o_target',
                 #                         's_id': 'source', 't_id' : 'target'}, inplace = True)
                    chunk_df = chunk_df[['source', 'target','i_source','i_target','Version','t_stmp']] 
                    file_save = edges_files[0]  + 'the_final'+n
                    print('to save',file_save)
                    chunk_df.to_csv(file_save, encoding='utf-8',index = False)
                    #data_l= data_l.append(M)
               # print(data_l[0])
                    #tt  =  p.map(, )
                    #print(M[0:1])
                    p.close()
                    p.join()
                    
#