class Cat_Coord(object):
    def __init__(self, host, port):
        ########## network parameters ####################################
        self.host = host
        self.port = port
        # self.my_node_id = int(self.host.split(".")[-1]) % (2 ** M)

        ########## Coord parameters #####################################
        self.TID=0
        self.TID_LIST={} #client_host:TID
        self.T_PAR={} # Participants for each TID; TID: [Participants' hosts]
        self.T_COM_VOTE={} #TID: [Participants hosts with vote yes]

        #############Deadlock Detectiion parameters!!!!!!!!!!!##############

        self.WAIT_GRAPH_REC=[]
        ###########!!!!!!!!!!!!!!!!!###########################

    def multicast(self, TID,cmd):  # method for multicast msg
        for host in self.T_PAR[TID]:
            self.client(host, self.port, cmd)  # pack the msg as a client socket to send

    #######################!!!!!!!!!!!!!!!!!!!#########################
    def multicast_wait_graph(self,cmd):
        for ss in ['A','B','C','D','E']:
            host=SER_CAT[ss]
            self.client(host, self.port, cmd)
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
                recv_data = conn.recv(1024)
                if not recv_data:  # recv ending msg from client
                    break

                recv_data_list = recv_data.split(" ")

                com=recv_data_list[0]
                if com == "BEGIN":
                    self.TID= self.TID+1
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
                                                  # Send for target(A~E) = TID:SET:x:1 2 3
                                                  kwargs={})
                    send_thr.start()

                    return_thr = threading.Thread(target=self.client,
                                                  args=(addr[0], self.port,
                                                        "SET OK"),
                                                  # return:set ok
                                                  kwargs={})
                    return_thr.start()

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
                    TID =self.TID_LIST[addr[0]]
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

                elif com == "ABORT":
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

                    if cmd.split("=")[-1]=="NOT FOUND":
                        send_thr = threading.Thread(target=self.client,
                                                    args=(host_tar, self.port,
                                                          "NOT FOUND:" + str(TID) ),
                                                    # send for client "NOT FOUND:TID"
                                                    kwargs={})
                        send_thr.start()

                        multicast_thr = threading.Thread(target=self.multicast,
                                                         args=(TID,
                                                               str(TID) + ":" + "ABORT"),
                                                         # Send for target TID:ABORT
                                                         kwargs={})
                        multicast_thr.start()
                        sys.stderr.write("Received  NOT FOUND and multicast ABORT\n")

                        returnback_thr = threading.Thread(target=self.client,
                                                      args=(host_tar, self.port,
                                                            "Aborted:" + str(TID)),
                                                      kwargs={})
                        returnback_thr.start()

                    else:
                        send_thr = threading.Thread(target=self.client,
                                                args=(host_tar, self.port,
                                                "GOT:"+str(TID)+":"+cmd),
                                                # send for client "GOT:TID:cmd(SID.obj=val1 val2 val3)"
                                                kwargs={})
                        send_thr.start()
                        sys.stderr.write("Received " + recv_data + " and send back to client\n")


                ########################!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!#######################
                elif com =='WAIT_GRAPH': #"EDGE+ 1:2,3"
                    # reconstruct the wait graph based on the received wait msg

                    self.WAIT_GRAPH_REC.append(recv_data_list[1])

                    if len(self.WAIT_GRAPH_REC)==5:

                        wait_graph_rec=deepcopy(self.WAIT_GRAPH_REC)
                        wait_graph = {}
                        for wait_info_long in wait_graph_rec:
                            for wait_info in wait_info_long.split("|"):
                                wait_graph = self.construct_graph(wait_info,wait_graph)
                        self.WAIT_GRAPH_REC=[]

                    TID_dead = self.cycle_detection(wait_graph)

                    if TID_dead:
                        #Found dead lock and abort TID_dead
                        multicast_thr = threading.Thread(target=self.multicast,
                                                         args=(int(TID_dead),
                                                               str(TID_dead) + ":" + "ABORT"),
                                                         # Send for target TID:ABORT
                                                         kwargs={})
                        multicast_thr.start()
                        sys.stderr.write("Detected deadlock and decide to ABORT"+str(TID_dead)+"\n")
                        return_thr = threading.Thread(target=self.client,
                                                      args=(addr[0], self.port,
                                                            "Aborted:" + str(TID_dead)),
                                                      kwargs={})
                        return_thr.start()


                ############################!!!!!!!!!!!!!!!!!!!!!!!!!!!#####################
                else:
                    sys.stderr.write("Coord received wrong msg "+ recv_data+"\n")

            conn.close()  # close client socket

    ####################!!!!!!!!!!!!!#######################
    def ask_wait_graph(self): # Heartbeat main method
        prev_time = time.time()*1000
        #slp_time = (float(self.period)/1000)/20
        while True:
            #time.sleep(slp_time) # delay for checking
            cur_time = time.time()*1000
            if(cur_time-prev_time>self.period): #send heartbeating every period
                prev_time = cur_time
                self.multicast_wait_graph("ASK_WAIT_GRAPH")
    ####################!!!!!!!!!!!!!#######################

