# distributed-server-IB
distributed server over infiniband


we build a distributes server over infiniband. the main idea is that the client think he is talking only to 1 server, while in reality, when he want to write or read, first he address to the 'indexer', which address him to the right server. we build this project during the last few weeks of the semester. there are few issues need to take care. the more interesting things to do is to make the indexer more as 'balancer'. to make sure there is no server being address all the time, no bottle necks, and any thing can think of


we did this project during course and the guides came from mellanox. we get 96 for this prodect only
