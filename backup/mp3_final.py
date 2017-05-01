#!/usr/bin/pypy
'''
ECE428: Distributed System
Machine Problem 3
Author: Rui Xia, Youjie Li
Date: May.1.2017
'''

import socket
import threading
import time
import thread
import sys
from copy import deepcopy

class Cat_Client(object):
    def __init__(self, host, port):
        ########## network parameters ####################################
        self.host = host
        self.port = port
        ########## client parameters #####################################
        self.tran_start=False

    def basic_multicast(self, cmd):  # method for multicast msg
        for i in range(10):
            host_remote = 'sp17-cs425-g07-' + str(i + 1).zfill(2) + '.cs.illinois.edu'
            self.client(host_remote, self.port, cmd)  # pack the msg as a client socket to send

    def wait_input(self):  # method for take input msg
        while True:
            try:
                cmd = raw_input()
            except EOFError:
                sys.stderr.write("EOF\n")
                return 0

            cmd_list = cmd.split(" ")
            host_tar = socket.gethostbyname(SER_CAT["Coord"])

            if len(cmd_list) < 1:
                sys.stderr.write("No Command\n")
            else:

                com = cmd_list[0]

                if com == "BEGIN":
                    if not self.tran_start:
                        self.tran_start=True
                        self.client(host_tar, self.port, cmd)
                    else:
                        sys.stderr.write("Invalid BEGIN\n")

                elif com == "SET":
                    if not self.tran_start:
                        sys.stderr.write("Please begin a trainsaction\n")
                    elif len(cmd_list)<3:
                        sys.stderr.write("Invalid SET\n")
                    else:
                        ss = cmd_list[1].split(".")[0]
                        if ss == 'A' or ss == 'B' or ss == 'C' or ss == 'D' or ss == 'E':
                            self.client(host_tar, self.port, cmd)
                        else:
                            sys.stderr.write("Invalid Server\n")

                elif com == "GET":
                    if not self.tran_start:
                        sys.stderr.write("Please begin a transaction\n")
                    elif len(cmd_list) != 2:
                        sys.stderr.write("Invalid GET\n")
                    else:
                        ss = cmd_list[1].split(".")[0]
                        if ss == 'A' or ss == 'B' or ss == 'C' or ss == 'D' or ss == 'E':
                            self.client(host_tar, self.port, cmd)
                        else:
                            sys.stderr.write("Invalid Server\n")

                elif com == "COMMIT" or com == "ABORT":
                    if not self.tran_start:
                        sys.stderr.write("Please Begin a Trainsaction\n")
                    else:
                        #self.tran_start = False
                        self.client(host_tar, self.port, cmd)

                else:
                    sys.stderr.write("Invalid Command\n")

                    # self.client(self.host, self.port,cmd)

    def client(self, host, port, cmd):  # method for client socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # name = CONNECTION_LIST[socket.gethostname()]  # find current machine name
        try:
            s.connect((host, port))  # connect to server
        except:
            # print host + ": Not Online" #debug
            s.close()
            return -1

        try:
            s.sendall(cmd)  # send message to sever
        except:
            s.close()
            return -1

        s.close()
        return 0

    def server(self):
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ss.bind((self.host, self.port))
        ss.listen(100)

        while True:
            conn, addr = ss.accept()
            # print 'Connected by ', addr

            while True:
                recv_data = conn.recv(4096)
                if not recv_data:  # recv ending msg from client
                    break

                recv_data_list=recv_data.split(":")

                # self.Hashing(recv_data, addr)
                if recv_data_list[0] == "TID":
                    print "OK"
                    sys.stdout.flush()
                    sys.stderr.write("Your TID is " + recv_data_list[1]+"\n")

                elif recv_data_list[0] == "SET OK":
                    print "OK"
                    sys.stdout.flush()

                elif recv_data_list[0] == "Aborted":
                    print "ABORT"
                    sys.stdout.flush()
                    self.tran_start = False
                    sys.stderr.write("Abort TID " + recv_data_list[1]+"\n")

                elif recv_data_list[0] == "Committed":
                    print "COMMIT OK"
                    sys.stdout.flush()
                    self.tran_start = False
                    sys.stderr.write("COMMIT TID " + recv_data_list[1]+"\n")

                elif recv_data_list[0] =="GOT": #"GOT:TID:cmd(SID.obj=val1 val2 val3)"
                    cmd = recv_data_list[2:]
                    cmd = ":".join(cmd)
                    #TID =recv_data_list[1]
                    if "=" in cmd:
                        indx = cmd.index("=")
                        res = cmd[:indx] + " = " + cmd[indx+1:]
                        print res
                        sys.stdout.flush()
                    else:
                        sys.stderr.write(cmd+"\n")

                elif recv_data_list[0] == "NOT FOUND":  # "NOT FOUND:TID"
                    TID = recv_data_list[1]
                    print "NOT FOUND"
                    sys.stdout.flush()
                    #self.tran_start = False
                    #sys.stderr.write("recived NOT FOUND from TID " + TID + "\n")

                elif recv_data_list[0] == "SOT": #"SOT:TID:OK/Wait4"
                    set_result = recv_data_list[2]
                    if set_result == "OK":
                        print "OK"
                        sys.stdout.flush()
                    else:
                        sys.stderr.write(set_result+"\n")

                else:
                    sys.stderr.write("Client received wrong msg "+recv_data +"\n")

            conn.close()  # close client socket

class Cat_Coord(object):
    def __init__(self, host, port):
        ########## network parameters ####################################
        self.host = host
        self.port = port
        ########## Coord parameters #####################################
        self.TID=0
        self.TID_LIST={} #client_ip:TID
        self.T_PAR={} # Participants for each TID; TID: [Participants' hosts]
        self.T_COM_VOTE={} #TID: [Participants hosts with vote yes]
        self.SERVER_LIST=[]
        #############Deadlock Detectiion parameters!!!!!!!!!!!##############
        self.dl_detect_period = float(4000.0) #ms
        self.dl_detect_freq = 5 # every period
        self.WAIT_GRAPH_REC = []
        ###########!!!!!!!!!!!!!!!!!###########################

    def multicast(self, TID,cmd):  # method for multicast msg
        for host in self.T_PAR[TID]:
            self.client(host, self.port, cmd)  # pack the msg as a client socket to send

    #######################!!!!!!!!!!!!!!!!!!!#########################
    def multicast_wait_graph(self, cmd):
        for ss in ['A', 'B', 'C', 'D', 'E']:
            server_ip = socket.gethostbyname(SER_CAT[ss])
            self.client(server_ip, self.port, cmd)
    #######################!!!!!!!!!!!!!!!!!##########################

    def client(self, host, port, cmd):  # method for client socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # name = CONNECTION_LIST[socket.gethostname()]  # find current machine name
        try:
            s.connect((host, port))  # connect to server
        except:
            # print host + ": Not Online" #debug
            s.close()
            return -1

        try:
            s.sendall(cmd)  # send message to sever
        except:
            s.close()
            return -1

        s.close()
        return 0

    def server(self):
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ss.bind((self.host, self.port))
        ss.listen(100)

        while True:
            conn, addr = ss.accept()
            # print 'Connected by ', addr

            while True:
                recv_data = conn.recv(8192)
                if not recv_data:  # recv ending msg from client
                    break

                recv_data_list = recv_data.split(" ")

                com=recv_data_list[0]
                if com == "BEGIN":
                    self.TID = self.TID+1
                    self.TID_LIST[addr[0]] = self.TID
                    self.T_PAR[self.TID] = []

                    return_thr = threading.Thread(target=self.client,
                                                  args=(addr[0],self.port,
                                                        "TID:"+str(self.TID)),
                                                  # return:nodeid*returnvalue:timestamp
                                                  kwargs={})
                    # return_thr.daemon = True
                    return_thr.start()

                    sys.stderr.write("Received begin and finish allocate TID\n")

                    ###############################
                    if self.TID == 2:
                        t1 = threading.Thread(target=self.ask_wait_graph)
                        t1.daemon = True
                        t1.start()
                    ###############################

                elif com == "SET":
                    TID =self.TID_LIST[addr[0]]
                    target = recv_data_list[1].split(".")[0] #ABCDE
                    obj=recv_data_list[1].split(".")[1]

                    host_tar=socket.gethostbyname(SER_CAT[target])
                    if host_tar not in self.T_PAR[TID]:
                        self.T_PAR[TID].append(host_tar)

                    send_thr = threading.Thread(target=self.client,
                                                  args=(host_tar, self.port,
                                                        str(TID)+":SET:"+obj+":"+" ".join(recv_data_list[2:])),
                                                  # Send to server(A~E) = TID:SET:x:1 2 3
                                                  kwargs={})
                    send_thr.start()
                    sys.stderr.write("Received SET and send to corresponding server\n")

                elif com == "GET":
                    TID = self.TID_LIST[addr[0]]
                    target = recv_data_list[1].split(".")[0]
                    obj = recv_data_list[1].split(".")[1]

                    host_tar = socket.gethostbyname(SER_CAT[target])

                    if host_tar not in self.T_PAR[TID]:
                        self.T_PAR[TID].append(host_tar)

                    send_thr = threading.Thread(target=self.client,
                                                args=(host_tar, self.port,
                                                      str(TID) + ":GET:" + obj),
                                                # Send for target(A~E)= TID:GET:x
                                                kwargs={})
                    send_thr.start()

                    sys.stderr.write("Received GET and send to corresponding server\n")

                elif com == "COMMIT":
                    TID = self.TID_LIST[addr[0]] # get Client TID
                    # if empty Transaction, just Commit Ok
                    if len(self.T_PAR[TID]) == 0:
                        host_tar = -1
                        for key, value in self.TID_LIST.iteritems():
                            if value == TID:
                                host_tar = key
                                break

                        sendback_thr = threading.Thread(target=self.client,
                                                        args=(host_tar, self.port,
                                                              "Committed:" + str(TID)),
                                                        kwargs={})
                        sendback_thr.start()
                        sys.stderr.write("Received Empty COMMIT\n")

                    else: # normal commit
                        self.T_COM_VOTE[TID] = []

                        multicast_thr = threading.Thread(target=self.multicast,
                                                         args=(TID,
                                                               str(TID)+":"+ "canCommit?"),
                                                         # Send for target TID:canCommit?
                                                         kwargs={})
                        multicast_thr.start()
                        sys.stderr.write("Received "+ com+ " and multicast canCommit?\n")

                elif com=="VOTE": #VODE TID serverID
                    TID=recv_data_list[1]
                    SID=recv_data_list[-1]

                    sys.stderr.write("Received votes from "+SID+" for "+TID+"\n")
                    TID=int(TID)
                    self.T_COM_VOTE[TID].append(SID)

                    host_tar=-1
                    for key,value in self.TID_LIST.iteritems():
                        if value==TID:
                            host_tar= key
                            break

                    if len(self.T_COM_VOTE[TID])==len(self.T_PAR[TID]):
                        multicast_thr = threading.Thread(target=self.multicast,
                                                         args=(TID,str(TID) + ":" + "doCommit"),
                                                         # Send for target TID:doCommit
                                                         kwargs={})
                        multicast_thr.start()


                        sendback_thr = threading.Thread(target=self.client,
                                                      args=(host_tar, self.port,
                                                            "Committed:"+str(TID)),
                                                      kwargs={})
                        sendback_thr.start()
                        sys.stderr.write("Received all votes and multicast doCommit\n")

                elif com == "ABORT": # from Client
                    TID = self.TID_LIST[addr[0]]
                    self.T_COM_VOTE[TID] = []

                    multicast_thr = threading.Thread(target=self.multicast,
                                                     args=(TID,
                                                           str(TID) + ":" + "ABORT"),
                                                     # Send for target TID:ABORT
                                                     kwargs={})
                    multicast_thr.start()
                    sys.stderr.write("Received " + com + " and multicast ABORT\n")

                    return_thr = threading.Thread(target=self.client,
                                                  args=(addr[0], self.port,
                                                        "Aborted:" + str(TID)),
                                                  kwargs={})
                    return_thr.start()

                elif com=="GOTTEN": #GOTTEN TID SID.obj=value
                    TID=int(recv_data_list[1])
                    cmd=" ".join(recv_data_list[2:])
                    host_tar = -1

                    for key,value in self.TID_LIST.iteritems():
                        if value==TID:
                            host_tar= key
                            break

                    if cmd.split("=")[-1]=="NOT FOUND": # from Server
                        send_thr = threading.Thread(target=self.client,
                                                    args=(host_tar, self.port,
                                                          "NOT FOUND:" + str(TID) ),
                                                    # send to client "NOT FOUND:TID"
                                                    kwargs={})
                        send_thr.start()

                    else:
                        send_thr = threading.Thread(target=self.client,
                                                args=(host_tar, self.port,
                                                "GOT:"+str(TID)+":"+cmd),
                                                # send for client "GOT:TID:cmd(SID.obj=val1 val2 val3)"
                                                kwargs={})
                        send_thr.start()
                        sys.stderr.write("Received " + recv_data + " and send back to client\n")

                elif com=="SETTED": # SETTED TID OK/Wait4     from Server
                    TID=int(recv_data_list[1])
                    cmd=recv_data_list[2]
                    host_tar = -1
                    for key,value in self.TID_LIST.iteritems():
                        if value==TID:
                            host_tar= key
                            break

                    send_thr = threading.Thread(target=self.client,
                                                args=(host_tar, self.port,
                                                "SOT:"+str(TID)+":"+cmd),
                                                # send to client "SOT:TID:OK/Wait4"
                                                kwargs={})
                    send_thr.start()
                    sys.stderr.write("Received " + recv_data + " and send back to client\n")
                ########################!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!#######################
                elif com == 'WAIT_GRAPH':# "WAIT_GRAPH 1:2,3|2:3,4|..."
                    #reconstruct the wait graph based on the received wait msg
                    if len(recv_data_list) == 1: # empty graph
                        self.WAIT_GRAPH_REC.append("")
                    else:
                        self.WAIT_GRAPH_REC.append(recv_data_list[1])

                    if len(self.WAIT_GRAPH_REC) >= len(self.SERVER_LIST):
                        wait_graph_rec = deepcopy(self.WAIT_GRAPH_REC)
                        self.WAIT_GRAPH_REC = []
                        wait_graph = {}
                        for wait_info_long in wait_graph_rec:
                            for wait_info in wait_info_long.split("|"):
                                if wait_info != "":
                                    wait_graph = self.construct_graph(wait_info, wait_graph)

                        if len(wait_graph) != 0:
                            TID_dead = self.cycle_detection(wait_graph)
                            if TID_dead:
                                # Found dead lock and abort TID_dead
                                multicast_thr = threading.Thread(target=self.multicast,
                                                                 args=(int(TID_dead),
                                                                       str(TID_dead) + ":" + "ABORT"),
                                                                 # Send for target TID:ABORT
                                                                 kwargs={})
                                multicast_thr.start()
                                sys.stderr.write("DeadLock, ABORT TID:" + str(TID_dead) + "\n")

                                client_ip = -1
                                for key, value in self.TID_LIST.iteritems():
                                    if value == int(TID_dead):
                                        client_ip = key
                                        break

                                return_thr = threading.Thread(target=self.client,
                                                              args=(client_ip, self.port,
                                                                    "Aborted:" + str(TID_dead)),
                                                              kwargs={})
                                return_thr.start()

                ############################!!!!!!!!!!!!!!!!!!!!!!!!!!!#####################
                elif com == "ONLINE":
                    if recv_data_list[1] not in self.SERVER_LIST:
                        self.SERVER_LIST.append(recv_data_list[1])
                        sys.stderr.write(recv_data_list[1] + " is Online\n")
                else:
                    sys.stderr.write("Coord received wrong msg "+ recv_data+"\n")

            conn.close()  # close client socket

    ##################!!!!!!!!!!!!!!!!!!!!!################
    def cycle_detection(self, wait_graph):
        # Find circle and return nvertex
        # wait_graph: {T1: [T2,T3], T2: [T4], T4: [T1]}
        path = []

        def check_path(vertex):
            path.append(vertex)
            for wait_for in wait_graph.get(vertex, []):
                if wait_for in path or check_path(wait_for):
                    return True
            path.remove(vertex)
            return False

        for vertex in wait_graph:
            if check_path(vertex):
                return vertex

        return False

    def construct_graph(self, wait_info, wait_graph):
        wait_info_list = wait_info.split(":")
        if wait_info_list[0] in wait_graph:
            for ele in wait_info_list[1].split(","):
                if ele not in wait_graph[wait_info_list[0]]:
                    wait_graph[wait_info_list[0]].append(ele)
        else:
            wait_graph[wait_info_list[0]] = wait_info_list[1].split(",")

        return wait_graph

    def ask_wait_graph(self):  # Heartbeat Deadlock Detection
        prev_time = time.time() * 1000 # ms
        slp_time = (float(self.dl_detect_period)/1000)/self.dl_detect_freq
        while True:
            time.sleep(slp_time) # delay for checking
            cur_time = time.time() * 1000 # ms
            if cur_time - prev_time > self.dl_detect_period:  # send heartbeating every period
                prev_time = cur_time
                self.multicast_wait_graph("ALL:ASK_WAIT_GRAPH")
    ####################!!!!!!!!!!!!!#######################

class Cat_Server(object):
    def __init__(self, host, port):
        ########## network parameters ####################################
        self.host = host
        self.port = port
        self.coord_ip = socket.gethostbyname(SER_CAT["Coord"])
        ########## Server parameters #####################################
        self.OBJ_DIC={} # objname: object
        self.REQ_COMMIT_TID=[] # TID == string
        ###################################################################
        self.client(self.coord_ip, self.port,  "ONLINE "+CAT[socket.gethostname()])

    def client(self, host, port, cmd):  # method for client socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # name = CONNECTION_LIST[socket.gethostname()]  # find current machine name
        try:
            s.connect((host, port))  # connect to server
        except:
            # print host + ": Not Online" #debug
            s.close()
            return -1

        try:
            s.sendall(cmd)  # send message to sever
        except:
            s.close()
            return -1

        s.close()
        return 0

    def server(self):
        ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ss.bind((self.host, self.port))
        ss.listen(100)

        while True:
            conn, addr = ss.accept()
            # print 'Connected by ', addr

            while True:
                recv_data = conn.recv(2048)
                if not recv_data:  # recv ending msg from client
                    break

                recv_data_list = recv_data.split(":")
                TID = recv_data_list[0]
                com = recv_data_list[1]
                if com=="SET": #TID:SET:x:1 2 3
                    obj = recv_data_list[2]
                    val = ":".join(recv_data_list[3:])
                    self.execute_SET(TID, obj, val)
                    # debug #
                    sys.stderr.write(recv_data+"\n")
                    sys.stderr.write("[")
                    for key in self.OBJ_DIC.keys():
                        sys.stderr.write(key+",")
                    sys.stderr.write("]\n")

                elif com=="GET": #TID:GET:obj
                    obj = recv_data_list[-1]
                    self.execute_GET(TID, obj)
                    # debug #
                    sys.stderr.write(recv_data + "\n")
                    sys.stderr.write("[")
                    for key in self.OBJ_DIC.keys():
                        sys.stderr.write(key + ",")
                    sys.stderr.write("]\n")

                elif com=="canCommit?": #TID:canCommit?
                    print recv_data
                    # record which TID wants to Commit
                    self.REQ_COMMIT_TID.append(TID)

                    #  all object no more waiting for this TID: if Yes, the Vote; else else keep waiting
                    canVote = True
                    for each_obj in self.OBJ_DIC.itervalues():
                        if TID in each_obj.theWaitList.TID_cnt.keys():
                            canVote = False
                            break

                    if canVote: # Vote for the TID
                        self.REQ_COMMIT_TID.remove(TID)
                        self.client(self.coord_ip, self.port,
                                    "VOTE " + TID + " "+CAT[socket.gethostname()].split("-")[-1])
                                    #Vote TID SID
                    else:
                        # debug #
                        sys.stderr.write("Not Ready to Commit\n")

                elif com=="doCommit": #TID:doCommit
                    sys.stderr.write(recv_data+"\n")
                    self.exe_doCOMMIT_ABORT("doCOMMIT", TID)

                elif com=="ABORT": #TID:ABORT
                    sys.stderr.write(recv_data + "\n")
                    self.exe_doCOMMIT_ABORT("ABORT", TID)

                elif com=="ASK_WAIT_GRAPH":
                    # report wait graph
                    waitgraph_dic = {}
                    for each_obj in self.OBJ_DIC.values():
                        dic = each_obj.theWaitList.report_waitfor_Dic(each_obj)
                        #{'1': ['2', '3'], .....,}
                        if len(dic) != 0:
                            for key, value in dic.iteritems():
                                if key not in waitgraph_dic.keys():
                                    waitgraph_dic[key]=value
                                else: # UNION of all dependency
                                    waitgraph_dic[key] = list(set(waitgraph_dic[key]+value))

                    waitgraph_list = [] # ['1:2,3', .... ]
                    for key, value in waitgraph_dic.iteritems():
                        waitgraph_list.append(key+':'+','.join(value))

                    waitgraph_string = '|'.join(waitgraph_list) # "1:2,3|2:3,4|..."

                    self.client(self.coord_ip, self.port,
                                "WAIT_GRAPH " + waitgraph_string)
                                # WAIT_GRAPH 1:2,3|2:3,4|...

                else:
                    print "server received wrong msg "+ recv_data

            conn.close()  # close client socket

    ############################################################################
    def exe_doCOMMIT_ABORT(self, MODE, TID):
        if MODE != "doCOMMIT" and MODE != "ABORT":
            sys.stderr.write("MODE_Error: exe_doCOMMIT_ABORT\n")
            return -1

        sys.stderr.write("=== Before " + MODE + " ===\n")
        for each_obj in self.OBJ_DIC.values():
            each_obj.theWaitList.print_EntryList(each_obj.name)

        if MODE == "doCOMMIT":
            # commit value and unlock for each object of this TID
            for each_obj in self.OBJ_DIC.values():
                each_obj.commit_value(TID) # move val_tmp to commited_value if SET'ed
                each_obj.unlock(TID, "rw") # Unlock all objects locked by This TID

        elif MODE == "ABORT":
            # delete val_tmp and
            # delete entry with this TID in WaitList
            # unlock for each object of this TID
            for each_obj in self.OBJ_DIC.values():
                each_obj.delete_value_tmp(TID)
                each_obj.theWaitList.delete_TID(TID, each_obj.name)
                each_obj.unlock(TID, "rw")  # Unlock all objects locked by This TID

        #if (WaitList not empty): WaitList.run_ready_entry()
        for each_name, each_obj in self.OBJ_DIC.iteritems():
            if len(each_obj.theWaitList.EntryList) != 0:
                each_obj.theWaitList.run_ready_entry(each_name, each_obj, self)

        # recheck REQ_COMMIT_TID, if req_TID still exits, then see whether the req_TID canCommit
        for req_TID in self.REQ_COMMIT_TID:
            canVote = True
            for each_name, each_obj in self.OBJ_DIC.iteritems():
                if (req_TID in each_obj.theWaitList.TID_cnt.keys()):
                    canVote = False
                    break
            if canVote:  # Vote for the req_TID
                self.REQ_COMMIT_TID.remove(req_TID)
                self.client(self.coord_ip, self.port,
                            "VOTE " + req_TID + " " + CAT[socket.gethostname()].split("-")[-1])
                             # Vote TID SID

        sys.stderr.write("=== After "+MODE+" ===\n")
        for each_obj in self.OBJ_DIC.values():
            each_obj.theWaitList.print_EntryList(each_obj.name)

        return 0

    def execute_SET(self, TID, obj, val): #TID:SET:x:1 2 3
        set_result = "NA"
        if obj not in self.OBJ_DIC.keys():  # if not exit, create the object
            self.OBJ_DIC[obj] = Objects(TID, obj, val)
            set_result = "OK"

        else: # SET present object, may or may not block
            the_obj = self.OBJ_DIC[obj]
            # check this TID existence in the WaitList, Yes:block, this SET enter WaitList,
            if TID in the_obj.theWaitList.TID_cnt.keys() and the_obj.theWaitList.TID_cnt[TID] >= 1:
                the_obj.theWaitList.enter(TID, "SET", val, "inherit")
                sys.stderr.write("WaitList Entered:" + TID + "-SET-" + obj + "-inherit\n")
                wf = the_obj.theWaitList.EntryList[-1].waitfor
                lid = -1
                if wf == "WL":
                    lid = the_obj.WriteLock[0]
                else:
                    lid = "_".join(the_obj.ReadLock)
                set_result = "Wait4_" + wf + "_of_TID_" + lid

            else: # No: Check Read/Write Lock, proceed or enter the first entry to WaitList
                if the_obj.check_WLock() == 0 and the_obj.check_RLock()== 0:  # unlocked object
                    the_obj.set_lock(TID, "write")
                    the_obj.write_tmp_val(TID, val)
                    sys.stderr.write("Writelock Set:" + obj + "\n")
                    set_result = "OK"

                elif the_obj.check_WLock() == 0 and the_obj.check_RLock() != 0:  # ReadLocked object
                    if (TID in the_obj.ReadLock) and (the_obj.check_RLock() == 1): # Promote RL -> WL
                        the_obj.promote_lock(TID)
                        the_obj.write_tmp_val(TID, val)
                        sys.stderr.write("PromotedLock:" + obj + "\n")
                        set_result = "OK"
                    else:  # object Readlocked by other Trans or shared with other Trans
                        # add to waitlist
                        the_obj.theWaitList.enter(TID, "SET", val, "RL-self")
                        sys.stderr.write("WaitList Entered:" + TID + "-SET-" + obj + "-RL-self\n")
                        lid = "_".join(the_obj.ReadLock)
                        set_result = "Wait4_RL_of_TID_" + lid

                elif the_obj.check_WLock() != 0 and the_obj.check_RLock() == 0:  # WriteLocked object
                    if TID in the_obj.WriteLock:
                        the_obj.write_tmp_val(TID, val)
                        set_result = "OK"
                    else: # WriteLocked by other Trans
                        # add to waitlist
                        the_obj.theWaitList.enter(TID, "SET", val, "WL")
                        sys.stderr.write("WaitList Entered:" + TID + "-SET-" + obj + "-WL\n")
                        lid = "_".join(the_obj.WriteLock)
                        set_result = "Wait4_WL_of_TID_" + lid

                else:
                    sys.stderr.write("Wrong lock for object: " + obj + "\n")

        # reply SETTED to Coordinator
        return_thr = threading.Thread(target=self.client,
                                      args=(self.coord_ip, self.port,
                                            "SETTED " + TID + " " + set_result),
                                      # SETTED TID OK/Wait4
                                      kwargs={})
        return_thr.start()

    def execute_GET(self, TID, obj): #TID:GET:obj
        SID = CAT[socket.gethostname()].split("_")[-1]
        val_get = "NA"

        if obj not in self.OBJ_DIC.keys():
            val_get = "NOT FOUND"

        else: # GET present the_obj
            the_obj = self.OBJ_DIC[obj]
            # check this TID existence in the WaitList, Yes:block, this GET enter WaitList,
            if TID in the_obj.theWaitList.TID_cnt.keys() and the_obj.theWaitList.TID_cnt[TID] >= 1:
                the_obj.theWaitList.enter(TID, "GET", "--", "inherit")
                sys.stderr.write("WaitList Entered:"+TID+"-GET-"+obj+"-inherit\n")
                wf = the_obj.theWaitList.EntryList[-1].waitfor
                lid = -1
                if wf == "WL":
                    lid = the_obj.WriteLock[0]
                else:
                    lid = " ".join(the_obj.ReadLock)
                val_get = "Wait4_" + wf + "_of_TID_" + lid

            else:  # No: Check Read/Write Lock, proceed or enter the first entry to WaitList
                if the_obj.check_WLock() == 0 and the_obj.check_RLock() == 0:  # unlocked object
                    the_obj.set_lock(TID, "read")
                    val_get = the_obj.read_val(TID)
                    sys.stderr.write("Readlock Set:" + obj + "\n")

                elif the_obj.check_WLock() == 0 and the_obj.check_RLock() != 0:  # ReadLocked object
                    if TID in the_obj.ReadLock:
                        val_get = the_obj.read_val(TID)
                    else:
                        the_obj.set_lock(TID, "read_share")
                        val_get = the_obj.read_val(TID)
                        sys.stderr.write("Readlock Shared:"+obj+"\n")

                elif the_obj.check_WLock() != 0 and the_obj.check_RLock() == 0:  # WriteLocked object
                    if TID in the_obj.WriteLock:
                        val_get = the_obj.read_val(TID)
                    else:
                        # add to waitlist
                        val_get = "Wait4_WLock_of_TID_" + str(the_obj.WriteLock[0])
                        the_obj.theWaitList.enter(TID, "GET", "--", "WL")
                        sys.stderr.write("WaitList Entered:" + TID + "-GET-" + obj + "-WL\n")

                else:
                    sys.stderr.write("Wrong lock for object: " + obj + "\n")

        # reply GOTTEN value to Coordinator
        return_thr = threading.Thread(target=self.client,
                                      args=(self.coord_ip, self.port,
                                            "GOTTEN " + TID + " " + SID + "." + obj + "=" + val_get),
                                      # GOTTEN TID SID.obj=value
                                      kwargs={})
        return_thr.start()
        ###########################################################################

class Objects(object):
    def __init__(self, TID, name, val): # only first SET can create object
        self.name = name
        self.ReadLock = []
        self.WriteLock = [TID]
        self.value = "NOT FOUND" # Commited Value
        self.value_tmp = {TID:val}
        self.theWaitList = WaitList()

    def read_val(self,TID):
        if TID in self.value_tmp.keys():
            return self.value_tmp[TID]
        else:
            return self.value

    def write_tmp_val(self, TID,val):
        self.value_tmp[TID]=val

    def set_lock(self, TID, act):
        if act == "read": # unlocked obj
            self.ReadLock.append(TID)
        elif act == "read_share":
            self.ReadLock.append(TID)
        elif act == "write":
            self.WriteLock.append(TID)
        else:
            sys.stderr.write("Error:set_lock \n")

    def unlock(self, TID, act):
        if (act == "read"):
            if TID in self.ReadLock:
                self.ReadLock.remove(TID)
        elif (act == "write"):
            if TID in self.WriteLock:
                self.WriteLock.remove(TID)
        elif (act == "rw"):
            if TID in self.ReadLock:
                self.ReadLock.remove(TID)
            if TID in self.WriteLock:
                self.WriteLock.remove(TID)
        else:
            sys.stderr.write("Invalid Unlock Act\n")

    def promote_lock(self, TID):
        self.ReadLock.remove(TID)
        self.WriteLock.append(TID)

    def check_RLock(self):
        return len(self.ReadLock)

    def check_WLock(self):
        return len(self.WriteLock)

    def commit_value(self, TID):
        if TID in self.value_tmp.keys():
            self.value=self.value_tmp[TID]
            del self.value_tmp[TID]

    def delete_value_tmp(self, TID):
        if TID in self.value_tmp.keys():
            del self.value_tmp[TID]

class WaitEntry(object):
    def __init__(self, tid, cmd, value, waitfor):
        self.tid = str(tid)
        self.cmd = cmd # GET/SET
        self.value = value # SET value
        self.waitfor = waitfor # "RL-self" or "WL"

    def all_atr(self):
        return self.tid+"-"+self.cmd+"-"+self.value+"-"+self.waitfor

class WaitList(object):
    def __init__(self):
        self.EntryList = [] # [WaitEntry1, WaitEntry2 ...]
        self.TID_cnt = {} # {"TID":# of cmd in the WaitList} : Nonexsit or >=1

    def enter(self, TID, CMD, VAL, WAIT4):
        if WAIT4 == "inherit":
            wait4_parent = ""
            for entry in self.EntryList:
                if entry.tid == TID:
                    wait4_parent = entry.waitfor
                    break

            if wait4_parent == "":
                sys.stderr.write("Error: Enter WailtList, Failed Inherit Wait4\n")
            else:
                self.EntryList.append(WaitEntry(TID, CMD, VAL, wait4_parent))
                self.TID_cnt[TID] += 1

        elif WAIT4 == "RL-self" or WAIT4 == "WL": # first entry of this TID
            self.EntryList.append(WaitEntry(TID, CMD, VAL, WAIT4))
            self.TID_cnt[TID] = 1

        else:
            sys.stderr.write("Error: Enter WailtList\n")

    def delete_TID(self, TID, host_name):
        # Delete entries belong to this TID in EntryList
        indx_to_del = []
        for idx, entry in enumerate(self.EntryList):
            if entry.tid == TID:
                indx_to_del.append(idx)
        #
        if len(indx_to_del) != 0:
            for indx in indx_to_del:
                self.EntryList[indx] = -1
            for _ in range(len(indx_to_del)):
                self.EntryList.remove(-1)

        # Delete this TID from TID_cnt
        if TID in self.TID_cnt.keys():
            del self.TID_cnt[TID]

        sys.stderr.write(str(TID) + " deleted from " + host_name + "\n")

    def print_EntryList(self, host_name):
        sys.stderr.write("-- WaitList of " + host_name + " --\n")
        for entry in self.EntryList:
            sys.stderr.write(entry.all_atr()+"\n")
        sys.stderr.write("TID_cnt:\n")
        for key, value in self.TID_cnt.iteritems():
            sys.stderr.write(key+":"+str(value)+"\n")
        sys.stderr.write("-----------------------\n")

    def run_one_entry(self, my_indx, entry, host_name, host_obj, host_Server):
        executed = False
        promoted = False
        # check this TID existence in the Pre_WaitList,
        pre_myTID_exist = False
        pre_waitfor = "NA"
        for i in range(my_indx): # Pre_WaitList
            if entry.tid == self.EntryList[i].tid:
                pre_myTID_exist = True
                pre_waitfor = self.EntryList[i].waitfor
                break

        # Yes: keep wait, Inherit waitfor lock
        if pre_myTID_exist:
            entry.waitfor = pre_waitfor
            sys.stderr.write("WaitEntry Keep Wait:" + entry.all_atr() + "\n")

        else:  # No: Check Read/Write Lock, Yes: proceed, No: keep wait, Update waitfor
            if entry.cmd == "SET":
                set_result = "NA"
                if host_obj.check_WLock() == 0 and host_obj.check_RLock() == 0:  # unlocked object
                    host_obj.set_lock(entry.tid, "write")
                    sys.stderr.write("Writelock Set:" + host_name + "\n")
                    host_obj.write_tmp_val(entry.tid, entry.value)
                    executed = True
                    set_result = "OK"
                    sys.stderr.write("WaitEntry Executed:"+ entry.all_atr() + "\n")

                elif host_obj.check_WLock() == 0 and host_obj.check_RLock() != 0:  # ReadLocked object
                    if (entry.tid in host_obj.ReadLock) and (host_obj.check_RLock() == 1):  # Promote RL -> WL
                        host_obj.promote_lock(entry.tid)
                        host_obj.write_tmp_val(entry.tid, entry.value)
                        executed = True
                        promoted = True
                        set_result = "OK"
                        sys.stderr.write("WaitEntry Promoted:" + entry.all_atr()+ "\n")
                    else:  # object Readlocked by other Trans or shared with other Trans
                        # update entry's waitfor lock
                        entry.waitfor = "RL-self"
                        set_result = "Wait4_RLock_of_TID_" + "_".join(host_obj.ReadLock)
                        sys.stderr.write("WaitEntry Keep Wait:" + entry.all_atr()+ "\n")

                elif host_obj.check_WLock() != 0 and host_obj.check_RLock() == 0:  # WriteLocked object
                    if entry.tid in host_obj.WriteLock:
                        host_obj.write_tmp_val(entry.tid, entry.value)
                        executed = True
                        set_result = "OK"
                        sys.stderr.write("WaitEntry Executed:" + entry.all_atr() + "\n")
                    else:  # WriteLocked by other Trans
                        # update entry's waitfor lock
                        entry.waitfor = "WL"
                        set_result = "Wait4_WLock_of_TID_" + str(host_obj.WriteLock[0])
                        sys.stderr.write("WaitEntry Keep Wait:" + entry.all_atr() + "\n")

                else:
                    sys.stderr.write("SET:Wrong lock for object: " + host_name + "\n")
                    sys.stderr.write("ReadLock:"+" ".join(host_obj.ReadLock)+"\n")
                    sys.stderr.write("WriteLock:" + " ".join(host_obj.WriteLock) + "\n")

                # reply SETTED to Coordinator
                return_thr = threading.Thread(target=host_Server.client,
                                              args=(host_Server.coord_ip, host_Server.port,
                                                    "SETTED " + entry.tid + " " + set_result),
                                              # SETTED TID OK/Wait4
                                              kwargs={})
                return_thr.start()

            elif entry.cmd == "GET":
                SID = CAT[socket.gethostname()].split("_")[-1]
                val_get = "NA"

                if host_obj.check_WLock() == 0 and host_obj.check_RLock() == 0:  # unlocked object
                    val_get = host_obj.read_val(entry.tid) # value = "NOT FOUND" possible
                    if val_get == "NOT FOUND":
                        pass
                    else: # object exits
                        host_obj.set_lock(entry.tid, "read")
                        sys.stderr.write("Readlock Set:" + host_name + "\n")
                    executed = True
                    sys.stderr.write("WaitEntry Executed:" + entry.all_atr() + "\n")

                elif host_obj.check_WLock() == 0 and host_obj.check_RLock() != 0:  # ReadLocked object
                    if entry.tid in host_obj.ReadLock:
                        val_get = host_obj.read_val(entry.tid)
                        executed = True
                        sys.stderr.write("WaitEntry Executed:" + entry.all_atr() + "\n")
                    else:
                        host_obj.set_lock(entry.tid, "read_share")
                        val_get = host_obj.read_val(entry.tid)
                        executed = True
                        sys.stderr.write("Readlock Shared:" + host_name + "\n")
                        sys.stderr.write("WaitEntry Executed:" + entry.all_atr() + "\n")

                elif host_obj.check_WLock() != 0 and host_obj.check_RLock() == 0:  # WriteLocked object
                    if entry.tid in host_obj.WriteLock:
                        val_get = host_obj.read_val(entry.tid)
                        executed = True
                        sys.stderr.write("WaitEntry Executed:" + entry.all_atr()+ "\n")
                    else:
                        # update entry's waitfor lock
                        entry.waitfor = "WL"
                        sys.stderr.write("WaitEntry Keep Wait:" + entry.all_atr() + "\n")
                        val_get = "Wait4_WLock_of_TID_" + str(host_obj.WriteLock[0])

                else:
                    sys.stderr.write("GET:Wrong lock for object: " + host_name + "\n")
                    sys.stderr.write("ReadLock:" + " ".join(host_obj.ReadLock) + "\n")
                    sys.stderr.write("WriteLock:" + " ".join(host_obj.WriteLock) + "\n")

                # Corner Case: GET: NOT FOUND
                if val_get == "NOT FOUND":
                    host_obj.ReadLock=[]
                    host_obj.WriteLock=[]
                    host_obj.value_tmp = {}
                    executed = True
                    sys.stderr.write("Corner Case: GET: NOT FOUND\n")

                # reply GOTTEN value to Coordinator
                return_thr = threading.Thread(target=host_Server.client,
                                              args=(host_Server.coord_ip, host_Server.port,
                                                    "GOTTEN " + entry.tid + " " + SID + "." + host_name + "=" + val_get),
                                              # GOTTEN TID SID.obj=value
                                              kwargs={})
                return_thr.start()

            else:
                sys.stderr.write("Error: run_one_entry\n")

        return (executed, promoted)

    def run_ready_entry(self, host_name, host_obj, host_Server):
        # find the first ready entry (WL == empty) (RL-selfID == empty)
        first_ready_indx = -1
        for idx, entry in enumerate(self.EntryList):
            RL_tmp = deepcopy(host_obj.ReadLock)
            WL_tmp = deepcopy(host_obj.WriteLock)
            if entry.waitfor == "RL-self":
                if entry.tid in RL_tmp:
                    RL_tmp.remove(entry.tid)
                if len(RL_tmp)==0: # RL-selfID == empty
                    first_ready_indx = idx
                    break
            elif entry.waitfor == "WL":
                if len(WL_tmp)==0: # WL == empty
                    first_ready_indx = idx
                    break
            else:
                sys.stderr.write("Error: Find First Ready Entry\n")

        if first_ready_indx == -1: # no entry is ready
            sys.stderr.write("WaitList No Entry is Ready\n")
            return -1
        else:
            sys.stderr.write("First Ready Indx="+str(first_ready_indx)+"\n")

        # run_one_entry from the first ready entry to the end
        executed_indx = []
        promoted_indx = []
        for indx in range(first_ready_indx, len(self.EntryList)):
            executed, promoted = self.run_one_entry(indx, self.EntryList[indx], host_name, host_obj, host_Server)
            if executed:
                self.EntryList[indx].tid = -1
                executed_indx.append(indx)
            if promoted:
                promoted_indx.append(indx)

        # Delete executed index in EntryList
        if len(executed_indx) != 0:
            for indx in executed_indx:
                self.EntryList[indx] = -1
            for _ in range(len(executed_indx)):
                self.EntryList.remove(-1)
        else:
            sys.stderr.write("WaitList No Entry Executed\n")

        #if (promote == True) Promote the RL -> WL in Waitfor Column
        if len(promoted_indx) != 0:
            for entry in self.EntryList:
                if entry.waitfor == "RL-self":
                    entry.waitfor = "WL"
            sys.stderr.write("WaitList Promoted\n")
        else:
            sys.stderr.write("WaitList No Promotion\n")

        # Recount the TID_cnt
        self.TID_cnt = {}
        for entry in self.EntryList:
            if entry.tid not in self.TID_cnt:
                self.TID_cnt[entry.tid] = 1
            else:
                self.TID_cnt[entry.tid] += 1

        return 0

    def report_waitfor_Dic(self, host_obj): # return {'1':['2','3'], ....., }
        if len(self.TID_cnt) == 0:
            return {}
        else:
            dic = {}
            for tid in self.TID_cnt.keys():
                wf = ""
                for entry in self.EntryList:
                    if entry.tid == tid:
                        wf = entry.waitfor
                        break
                if wf == "RL-self":
                    RL_self = deepcopy(host_obj.ReadLock)
                    RL_self = list(set(RL_self))
                    if tid in RL_self:
                        RL_self.remove(tid)
                    dic[tid] = RL_self
                elif wf == "WL":
                    WL = deepcopy(host_obj.WriteLock)
                    WL = list(set(WL))
                    dic[tid] = WL
                else:
                    sys.stderr.write("Error:report_waitfor_Dic\nwf="+wf+"\n")

            return dic

                ############################# main code #######################################
if __name__ == "__main__":
    user_port = 9999  # port for message input
    host = socket.gethostbyname(socket.gethostname())
    CAT = {"sp17-cs425-g07-01.cs.illinois.edu": "Server_A",
           "sp17-cs425-g07-02.cs.illinois.edu": "Server_B",
           "sp17-cs425-g07-03.cs.illinois.edu": "Server_C",
           "sp17-cs425-g07-04.cs.illinois.edu": "Server_D",
           "sp17-cs425-g07-05.cs.illinois.edu": "Server_E",
           "sp17-cs425-g07-06.cs.illinois.edu": "Coord_0",
           "sp17-cs425-g07-07.cs.illinois.edu": "Client_1",
           "sp17-cs425-g07-08.cs.illinois.edu": "Client_2",
           "sp17-cs425-g07-09.cs.illinois.edu": "Client_3"}

    SER_CAT = {"A": "sp17-cs425-g07-01.cs.illinois.edu",
               "B": "sp17-cs425-g07-02.cs.illinois.edu",
               "C": "sp17-cs425-g07-03.cs.illinois.edu",
               "D": "sp17-cs425-g07-04.cs.illinois.edu",
               "E": "sp17-cs425-g07-05.cs.illinois.edu",
               "Coord": "sp17-cs425-g07-06.cs.illinois.edu"}

    # launch different classes according to its domain name
    catog = CAT[socket.gethostname()].split("_")
    user_port = 9999  # port for message input

    if catog[0] == "Server":
        sys.stderr.write("_".join(catog) + " Started\n")
        cat_node = Cat_Server(host, user_port)
        t1 = threading.Thread(target=cat_node.server)
        t1.daemon=True
        t1.start()

    elif catog[0] == "Coord":
        sys.stderr.write("_".join(catog) + " Started\n")
        cat_node = Cat_Coord(host, user_port)
        t1 = threading.Thread(target=cat_node.server)
        t1.daemon = True
        t1.start()

    elif catog[0] == "Client":
        sys.stderr.write("_".join(catog) + " Started\n")
        cat_node = Cat_Client(host, user_port)
        t1 = threading.Thread(target=cat_node.wait_input)  # thread for client (send msg)
        t2 = threading.Thread(target=cat_node.server)
        t1.daemon=True
        t2.daemon=True
        t2.start()
        t1.start()
    else:
        sys.stderr.write("CAT Error\n")
        exit(0)

    while True:
        pass






