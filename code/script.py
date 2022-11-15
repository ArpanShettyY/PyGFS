import uuid
import os
import time
import socket
import subprocess
import rpyc
import threading

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

# gives free port to start server on
def get_free_port():
	s = socket.socket()
	s.bind(('', 0))
	port=s.getsockname()[1]
	s.close()
	return port

# starts a server which acts as GFS master
def create_master():
	port=get_free_port()
	pinfo=subprocess.Popen(["python3", "master.py",str(port)])
	master.append(port)
	master.append(pinfo)
	print("Master started")
	
# starts a server which acts as GFS Chunkserver	
def create_chunkserver(cid=None):
	if cid==None:
		cid=str(uuid.uuid1().int)
	port=get_free_port()
	pinfo=subprocess.Popen(["python3", "chunkserver.py",str(port),str(cid)])
	chunkservers[cid]=[port,pinfo]
	
	# if there is a master server running, register this chunkserver to it
	if master!=[]:
		time.sleep(10)
		c = rpyc.connect("localhost", master[0])
		c.root.register(cid,port)
		c.close()
	print("New Chunkserver started with ID"+cid)
	
	return cid,port
	
# Stop the master server
def delete_master():
	master[1].terminate()
	master[1].wait()
	master.clear()
	print("Master has been stopped")

# Stop the Chunkserver mentioned
def delete_chunkserver(cid):
	chunkservers[cid][1].terminate()
	chunkservers[cid][1].wait()
	del chunkservers[cid]
	print("Chunkserver with ID "+cid+" stopped")	

# Provides options to manipulate GFS servers
def debug_options():
	while(True):
		print("Options:\n1.Start Chunkserver\n2.Stop Chunkserver\n3.Start Master\n4.Stop Master\n5.Show all servers\n6.Stop Script\n")
		op=int(input())
		if(op==1):
			print("Enter ID or type random")
			cid=input()
			if cid in chunkservers.keys():
				print("Chunkserver already running")
			else:
				if cid=="random":
					create_chunkserver()
				else:
					create_chunkserver(cid)
				
		elif(op==2):
			print("Choose Chunkserver to stop:")
			for csID in chunkservers.keys():
				print(csID)
			cid=input()
			if cid not in chunkservers.keys():
				print("Chunkserver not running")
			else:
				delete_chunkserver(cid)			
			
		elif(op==3):
			if master!=[]:
				print("Master is already running")
			else:
				create_master()
		elif(op==4):
			if master==[]:
				print("Master is not running")
			else:
				delete_master()
		elif(op==5):
			print("Running servers:")
			if master!=[]:
				print("Master Server on port",master[0])
			print("Chunkservers:")
			print("ID\tPort")
			for cid in chunkservers.keys():
				print(cid,"\t",chunkservers[cid][0])
		elif(op==6):
			if master!=[]:
				delete_master()
			tmp=list(chunkservers.keys())
			for cid in tmp:
				delete_chunkserver(cid)
			return
			
		else:
			print("Wrong Option")
		time.sleep(3)
		print("\n\n\n\n")


class ScriptService(rpyc.Service):
	def exposed_start_chunkserver(self):
	        return create_chunkserver()
	        
	def exposed_get_chunkserver_details(self):
		data={}
		for s in chunkservers.keys():
			data[s]=[chunkservers[s][0],0]
		return data
		
	def exposed_get_master_details(self):
		return master[0]

master=[]
chunkservers={}

chunkserver_disk="//home//pes1ug19cs086//Desktop//PyGFS//chunkserver-disk"
# start all the available chunkservers
for cs in os.listdir(chunkserver_disk):
	create_chunkserver(cs)
	
# if more servers required start new chunkservers
if len(os.listdir(chunkserver_disk))<7:
	for i in range(7-len(os.listdir(chunkserver_disk))):
		create_chunkserver()
time.sleep(5)
# start the master server
create_master()

thread = threading.Thread(target=debug_options)
thread.start()

if __name__ == "__main__":
	from rpyc.utils.server import ThreadedServer
	t = ThreadedServer(ScriptService, port=18861)
	t.start()
