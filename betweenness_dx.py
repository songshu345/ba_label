from random import sample
import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from conf.file_conf import root_path  # 路径查找

# 边文件（分季度）
dd_betweeness = root_path +'dx_yx_data/yx_q2_label_edge_0524_1.txt'


# 获取网络的边
G = nx.read_edgelist(dd_betweeness)
print(G)
# 原数据路径：WormNet.v3.benchmark.txt "D:/treasture/cloud_ama/wordcloudtxtdata/network/data/code.txt"
#
# remove randomly selected nodes (to make example fast)
# num_to_remove = int(len(G) / 1.5)
# nodes = sample(list(G.nodes), num_to_remove)
# G.remove_nodes_from(nodes)

# remove low-degree nodes
# low_degree = [n for n, d in G.degree() if d < 0.001]
# G.remove_nodes_from(low_degree)

# largest connected component
components = nx.connected_components(G)
# largest_component = max(components, key=len)
# H = G.subgraph(largest_component)
#

# 计算中介中心度
centrality = nx.betweenness_centrality(G, endpoints=False)  # endpoints终点是否被计入
centrality = sorted(centrality.items(), key=lambda item: item[1], reverse=True)
# type(centrality) # list

output = []
centrality1 = []
for node in centrality:
    output.append(node[0])  # 员工ID
    centrality1.append(node[1])  # Betweeness

#
# compute community structure
# lpc = nx.community.label_propagation_communities(H)
# community_index = {n: i for i, com in enumerate(lpc) for n in com}
# #
# #### draw graph ####
# fig, ax = plt.subplots(figsize=(20, 15))
# pos = nx.spring_layout(H, k=0.15, seed=4572321)
# node_color = [community_index[n] for n in H]
# node_size = [v * 20000 for v in centrality.values()]
# nx.draw_networkx(
#     H,
#     pos=pos,
#     with_labels=False,
#     node_color=node_color,
#     node_size=node_size,
#     edge_color="gainsboro",
#     alpha=0.4,
# )
# #
# # Title/legend
# font = {"color": "k", "fontweight": "bold", "fontsize": 20}
# ax.set_title("Gene functional association network (C. elegans)", font)
# # Change font color for legend
# font["color"] = "r"
#
# ax.text(
#     0.80,
#     0.10,
#     "node color = community structure",
#     horizontalalignment="center",
#     transform=ax.transAxes,
#     fontdict=font,
# )
# ax.text(
#     0.80,
#     0.06,
#     "node size = betweeness centrality",
#     horizontalalignment="center",
#     transform=ax.transAxes,
#     fontdict=font,
# )
#
# # Resize figure for label readibility
# ax.margins(0.1, 0.05)
# fig.tight_layout()
# plt.axis("off")
# plt.show()
# 导出数据
dataframe = pd.DataFrame({'emp_code': output, 'centrality': centrality1})
# 增加极度标签
# dataframe['label'] = 'Q1'
dataframe.to_csv(r"D:\treasture\label_BA\yx_betweenness\yx_q2_betweenness_0524_1.txt", index=False, sep=',', mode='a',
                 header=True, encoding="utf_8_sig")
