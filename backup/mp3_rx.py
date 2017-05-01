'''
ECE428: Distributed System
Machine Problem 3
Author: Rui Xia, Youjie Li
Date: April.10.2017
'''

import socket
import threading
import time
import thread
import sys
import copy

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
            cmd = raw_input("")
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
                    elif cmd_list[1].split(".")[0]>'E'or cmd_list[1].split(".")[0]<'A':
                        sys.stderr.write("Invalid Server\n")
                    else:
                        self.client(host_tar, self.port,cmd)

                elif com == "GET":
                    if not self.tran_start:
                        sys.stderr.write("Please begin a transaction\n")
                    elif len(cmd_list) != 2:
                        sys.stderr.write("Invalid GET\n")
                    elif cmd_list[1].split(".")[0]>'E'or cmd_list[1].split(".")[0]<'A':
                        sys.stderr.write("Invalid Server\n")
                    else:
                        self.client(host_tar, self.port,cmd)

                elif com == "COMMIT" or com == "ABORT":
                    if not self.tran_start:
                        sys.stderr.write("Please Begin a Trainsaction\n")
                    else:
                        self.tran_start = False
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
                recv_data = conn.recv(1024)
                if not recv_data:  # recv ending msg from client
                    break

                recv_data_list=recv_data.split(":")

                # self.Hashing(recv_data, addr)
                if recv_data_list[0] == "TID":
                    sys.stderr.write("Your TID is " + recv_data_list[1]+"\n")

                elif recv_data_list[0] == "SET OK":
                    print "OK"

                elif recv_data_list[0] == "Aborted":
                    print "ABORT"
                    sys.stderr.write("Abort TID " + recv_data_list[1]+"\n")

                elif recv_data_list[0] == "Committed":
                    print "COMMIT OK"
                    sys.stderr.write("COMMIT TID " + recv_data_list[1]+"\n")

                elif recv_data_list[0] =="GOT": #"GOT:TID:cmd(SID.obj=val1 val2 val3)"
                    cmd = recv_data_list[-1]
                    TID =recv_data_list[1]
                    print cmd
                    sys.stderr.write("recived gotten value from TID " + TID+ "\n")

                elif recv_data_list[0] == "NOT FOUND":  # "NOT FOUND:TID"
                    TID = recv_data_list[1]
                    print "NOT FOUND"
                    self.tran_start = False
                    sys.stderr.write("recived NOT FOUND from TID " + TID + "\n")

                else:
                    sys.stderr.write("Client received wrong msg "+recv_data +"\n")

            conn.close()  # close client socket

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

    def multicast(self, TID,cmd):  # method for multicast msg
        for host in self.T_PAR[TID]:
            self.client(host, self.port, cmd)  # pack the msg as a client socket to send


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

                else:
                    sys.stderr.write("Coord received wrong msg "+ recv_data+"\n")

            conn.close()  # close client socket

class Cat_Server(object):
    def __init__(self, host, port):
        ########## network parameters ####################################
        self.host = host
        self.port = port
        ########## Server parameters #####################################
        self.OBJ_DIC={} # objname: object

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

                recv_data_list = recv_data.split(":")
                com = recv_data_list[1]

                if com=="SET": #TID:SET:x:1 2 3
                    TID = recv_data_list[0]
                    obj = recv_data_list[2]
                    val=" ".join(recv_data_list[3:])
                    excute_set(self, TID, obj, val)

                    # if obj not in self.OBJ_DIC.keys(): # if not exit, create the object
                    #     self.OBJ_DIC[obj] = Objects(TID, val)
                    #
                    # else: #
                    #     if self.OBJ_DIC[obj].check_RLock() == 0 and self.OBJ_DIC[obj].check_WLock() == 0: # unlocked object
                    #         self.OBJ_DIC[obj].set_lock(TID, "write")
                    #         self.OBJ_DIC[obj].write_tmp_val(TID, val)
                    #
                    #     elif self.OBJ_DIC[obj].check_WLock() == 0 and self.OBJ_DIC[obj].check_RLock()!=0: # ReadLocked object
                    #         if (TID in self.OBJ_DIC[obj].ReadLock) and (self.OBJ_DIC[obj].check_RLock()==1):
                    #             self.OBJ_DIC[obj].promote_lock(TID)
                    #             self.OBJ_DIC[obj].write_tmp_val(TID, val)
                    #         else: # object Readlocked by other Trans or shared with other Trans
                    #             sys.stderr.write(recv_data+" waits for ReadLock\n")
                    #             #add to waitlist
                    #             #self.WaitList.append(recv_data+"wait pattern")
                    #     elif self.OBJ_DIC[obj].check_WLock() != 0 and self.OBJ_DIC[obj].check_RLock()==0: # WriteLocked object
                    #         if TID in self.OBJ_DIC[obj].WriteLock:
                    #             self.OBJ_DIC[obj].write_tmp_val(TID, val)
                    #         else:
                    #             sys.stderr.write(recv_data + " waits for WriteLock\n")
                    #             # add to waitlist
                    #             #self.WaitList.append(recv_data + "wait pattern")
                    #     else:
                    #         sys.stderr.write("Wrong lock for object: "+ obj +"\n")
                    #
                    #
                    # sys.stderr.write(recv_data+"\n")
                    # print self.OBJ_DIC

                elif com=="GET": #TID:GET:obj
                    TID=recv_data_list[0]
                    obj=recv_data_list[-1]

                    # SID=CAT[socket.gethostname()].split("_")[-1]
                    # val_get = "NA"
                    #
                    # if obj not in self.OBJ_DIC.keys():
                    #     val_get = "NOT FOUND"
                    #
                    # else:
                    #     if self.OBJ_DIC[obj].check_WLock() == 0: # Unlocked object or ReadLocked object
                    #         self.OBJ_DIC[obj].set_lock(TID, "read")
                    #         val_get = self.OBJ_DIC[obj].read_val(TID)
                    #
                    #     elif self.OBJ_DIC[obj].check_WLock() != 0 and self.OBJ_DIC[obj].check_RLock() == 0: # WriteLocked object
                    #         if TID in self.OBJ_DIC[obj].WriteLock:
                    #             val_get = self.OBJ_DIC[obj].read_val(TID)
                    #         else:
                    #             sys.stderr.write(recv_data + " waits for WriteLock\n")
                    #             val_get = "Waiting_for_WriteLock_of_TID_" + str(self.OBJ_DIC[obj].WriteLock[0])
                    #             #add to waitlist
                    #             #self.WaitList.append(recv_data + "wait pattern")
                    #     else:
                    #         sys.stderr.write("Wrong lock for object: " + obj + "\n")
                    #
                    #
                    # return_thr = threading.Thread(target=self.client,
                    #                               args=(addr[0], self.port,
                    #                                     "GOTTEN "+TID+" "+ SID+"."+obj+"="+val_get),
                    #                                     #GOTTEN TID SID.obj=value
                    #                               kwargs={})
                    # return_thr.start()
                    #
                    # sys.stderr.write(recv_data + "\n")

                elif com=="canCommit?": #TID:canCommit?
                    print recv_data

                    return_thr = threading.Thread(target=self.client,
                                                  args=(addr[0], self.port,
                                                        "VOTE " + recv_data_list[0] + " "+CAT[socket.gethostname()].split("-")[-1]),
                                                        #Vote TID SID
                                                  kwargs={})
                    return_thr.start()

                elif com=="doCommit": #TID:doCommit
                    print recv_data
                    tid = recv_data_list[0]
                    # Tmp_Values -> Committed_Values
                    # Unlock all objects locked by This TID
                    for the_obj in self.OBJ_DIC.values():
                        the_obj.commit_value(tid)
                        the_obj.unlock(tid, "rw")

                elif com=="ABORT": #TID:ABORT
                    print recv_data
                    tid = recv_data_list[0]
                    # Unlock all objects locked by This TID
                    for the_obj in self.OBJ_DIC.values():
                        the_obj.unlock(tid, "rw")

                else:
                    print "server received wrong msg "+ recv_data

            conn.close()  # close client socket

    ##################################New!!!!!!!!!!!!!!!!!!###########################################
    def excute_set(self,TID,obj,val):
        if obj not in self.OBJ_DIC.keys():  # if not exit, create the object
            self.OBJ_DIC[obj] = Objects(TID, val)

        else:  #
            if self.OBJ_DIC[obj].check_RLock() == 0 and self.OBJ_DIC[obj].check_WLock() == 0:  # unlocked object
                self.OBJ_DIC[obj].set_lock(TID, "write")
                self.OBJ_DIC[obj].write_tmp_val(TID, val)

            elif self.OBJ_DIC[obj].check_WLock() == 0 and self.OBJ_DIC[obj].check_RLock() != 0:  # ReadLocked object
                if (TID in self.OBJ_DIC[obj].ReadLock) and (self.OBJ_DIC[obj].check_RLock() == 1):
                    self.OBJ_DIC[obj].promote_lock(TID)
                    self.OBJ_DIC[obj].write_tmp_val(TID, val)
                else:  # object Readlocked by other Trans or shared with other Trans
                    sys.stderr.write(recv_data + " waits for ReadLock\n")
                    # add to waitlist
                    # self.WaitList.append(recv_data+"wait pattern")
            elif self.OBJ_DIC[obj].check_WLock() != 0 and self.OBJ_DIC[
                obj].check_RLock() == 0:  # WriteLocked object
                if TID in self.OBJ_DIC[obj].WriteLock:
                    self.OBJ_DIC[obj].write_tmp_val(TID, val)
                else:
                    sys.stderr.write(recv_data + " waits for WriteLock\n")
                    # add to waitlist
                    # self.WaitList.append(recv_data + "wait pattern")
            else:
                sys.stderr.write("Wrong lock for object: " + obj + "\n")

        sys.stderr.write(recv_data + "\n")
        print self.OBJ_DIC

    def excute_get(self,TID,obj):
        val_get = "NA"
        SID = CAT[socket.gethostname()].split("_")[-1]

        if obj not in self.OBJ_DIC.keys():
            val_get = "NOT FOUND"

        else:
            if self.OBJ_DIC[obj].check_WLock() == 0: # Unlocked object or ReadLocked object
                self.OBJ_DIC[obj].set_lock(TID, "read")
                val_get = self.OBJ_DIC[obj].read_val(TID)

            elif self.OBJ_DIC[obj].check_WLock() != 0 and self.OBJ_DIC[obj].check_RLock() == 0: # WriteLocked object
                if TID in self.OBJ_DIC[obj].WriteLock:
                    val_get = self.OBJ_DIC[obj].read_val(TID)
                else:
                    sys.stderr.write(recv_data + " waits for WriteLock\n")
                    val_get = "Waiting_for_WriteLock_of_TID_" + str(self.OBJ_DIC[obj].WriteLock[0])
                    #add to waitlist
                    #self.WaitList.append(recv_data + "wait pattern")
            else:
                sys.stderr.write("Wrong lock for object: " + obj + "\n")


        return_thr = threading.Thread(target=self.client,
                                      args=(addr[0], self.port,
                                            "GOTTEN "+TID+" "+ SID+"."+obj+"="+val_get),
                                            #GOTTEN TID SID.obj=value
                                      kwargs={})
        return_thr.start()

        sys.stderr.write(recv_data + "\n")

    ####################################New!!!!!!!!!!!!!!!!!!!!#######################################


class Objects(object):
    def __init__(self, TID, val): # only first SET can create object
        self.ReadLock = []
        self.WriteLock = [TID]
        self.value = "NA"
        self.value_tem={TID:val}

    def read_val(self,TID):
        if TID in self.value_tem.keys():
            return self.value_tem[TID]
        else:
            return self.value

    def write_tmp_val(self, TID,val):
        self.value_tem[TID]=val

    def set_lock(self, TID, act):
        if (act == "read"):
            self.ReadLock.append(TID)
        elif (act == "write"):
            self.WriteLock.append(TID)

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

    def get_readlock(self):
        return self.ReadLock

    def get_writelock(self):
        return self.WriteLock

    def promote_lock(self, TID):
        self.ReadLock.remove(TID)
        self.WriteLock.append(TID)

    def check_RLock(self):
        return len(self.ReadLock)

    def check_WLock(self):
        return len(self.WriteLock)

    def commit_value(self, TID):
        self.value=self.value_tem[TID]
        del self.value_tem[TID]












#  ############################ main code #######################################
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
        print "_".join(catog) + " Started"
        cat_node = Cat_Server(host, user_port)
        t1 = threading.Thread(target=cat_node.server)
        t1.daemon=True
        t1.start()

    elif catog[0] == "Coord":
        print "_".join(catog) + " Started"
        cat_node = Cat_Coord(host, user_port)
        t1 = threading.Thread(target=cat_node.server)
        t1.daemon = True
        t1.start()

    elif catog[0] == "Client":
        print "_".join(catog) + " Started"
        cat_node = Cat_Client(host, user_port)
        t1 = threading.Thread(target=cat_node.wait_input)  # thread for client (send msg)
        t2 = threading.Thread(target=cat_node.server)
        t1.daemon=True
        t2.daemon=True
        t2.start()
        t1.start()
    else:
        print "CAT Error"
        exit(0)


    while True:
        pass






