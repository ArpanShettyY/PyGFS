import rpyc
import sys
import os
import threading
import time
import uuid
import pickle
import copy
import logging

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

# Persists the metadata present in master
def persist():
	f = open(fc_map_file, 'wb')     
	pickle.dump(fc_map,f)
	f.close()
	logger.info("Persisted File-Chunk Mapping")

# Used to send heartbeats from master to chunkserver 
def heartbeat(serverID,port):
	while True:
		try:
			# Attempt to connect to the chunkserver and send up-to date chunk info
			c = rpyc.connect("localhost", port)
			c.root.refresh_chunks(chunkservers[serverID][1])
			c.close()
		
		except:
			# Indicates Chunkserver is down, so remove its metadata
			logger.info("Detected chunkserver with ID "+serverID+" is down")
			for chunk in chunkservers[serverID][1]:
				valid_chunks[chunk].remove(serverID)
			del chunkservers[serverID]
			
			# Inform script of loss of Chunkserver and ask it to start a new one 
			c = rpyc.connect("localhost", 18861)
			c.root.start_chunkserver()
			c.close()
			return
			
		time.sleep(20)

# Register a new chunkserver with the master
def register_chunk_server(serverID,port):
	# Store metadata about the new chunkserver
	chunkservers[serverID]=[]
	chunkservers[serverID].append(port)
	chunkservers[serverID].append([])
	c = rpyc.connect("localhost", port)
	chunks=copy.deepcopy(c.root.chunk_list())
	c.close()
	for chunk in chunks:
		if chunk in valid_chunks.keys():
			valid_chunks[chunk].append(serverID)
			chunkservers[serverID][1].append(chunk)
	# Start sending heartbeats to this chunkserver
	thread = threading.Thread(target=heartbeat, args=(serverID, port))
	thread.start()
	logger.info("Registered Chunkserver with ID "+serverID)
	return

# Deletes a file at a later period
def lazy_delete_file(filename):
	time.sleep(50)
	# remove all metadata about the file and its chunks
	for chunk in fc_map[filename]:
		for server in valid_chunks[chunk]:
			chunkservers[server][1].remove(chunk)
		del valid_chunks[chunk]
	del fc_map[filename]
	logger.info("Deleted file "+filename)
	persist()
		
# Helps maintain a constant number of replicas
def rebalancing():
	while True:
		time.sleep(70)
		for chunk in valid_chunks.keys():
			# Find number of replicas available
			l=len(valid_chunks[chunk])
			# If excess replicas are present
			if l>3:
				r=l-3
				server_list=free_server(valid_chunks[chunk])
				server_list=server_list[-r:]
				# Delete replicas from highly used chunkservers to reduce load
				for s in server_list:
					delete_chunk_from_server(chunk,s)
				logger.info("Removed excess replica of chunk "+chunk)

			# If less number of replicas are present
			elif l<3:
				r=3-l
				server_list=list(chunkservers.keys())
				for s in valid_chunks[chunk]:
					server_list.remove(s)
				# Create new replicas in non-busy chunkservers to increase their usage
				server_list=free_server(server_list)[:r]
				primary_port=chunkservers[valid_chunks[chunk][0]][0]
				for s in server_list:
					valid_chunks[chunk].append(s)
					chunkservers[s][1].append(chunk)
					c = rpyc.connect("localhost", chunkservers[s][0])
					c.root.pull_chunk(chunk,primary_port)
					c.close()
					
				logger.info("Created new replicas for chunk "+chunk)	

# Clears metadata of a chunk from server				
def delete_chunk_from_server(chunk_id,server_id):
	valid_chunks[chunk_id].remove(server_id)
	chunkservers[server_id][1].remove(chunk_id)

# Arranges chunkserver from the least load to the highest load					
def free_server(server_list):
	storage=[]
	for i in server_list:
		storage.append(len(chunkservers[i][1]))
	S=[x for _,x in sorted(zip(storage,server_list))]
	return S
		

myDisk="//home//pes1ug19cs086//Desktop//PyGFS//master-disk"
logPath=os.path.join(myDisk, "logfile.log")
myPort = int(sys.argv[1])

# Defining the formatting of master logs
import logging.config
logging.basicConfig(filename=logPath, level=logging.INFO, format='%(asctime)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logger = logging.getLogger('')

logger.info("Master Started at Port "+str(myPort))


c = rpyc.connect("localhost", 18861)
cs_data=copy.deepcopy(c.root.get_chunkserver_details())
c.close()

logger.info("Recieved Available Chunkserver details")

# Reconstruct File-Chunk-Mapping from the persisted file
fc_map_file=os.path.join(myDisk, "file_chunk_map")
f = open(fc_map_file, 'rb')     
fc_map = pickle.load(f)
f.close()

logger.info("Retrieved File Chunk Mapping")


chunkservers={}
valid_chunks={}

# Reconstruct other metadata from File-Chunk-Mapping
for f in fc_map.keys():
	for chunk in fc_map[f]:
		valid_chunks[chunk]=[]


for cs in cs_data.keys():
	register_chunk_server(cs,cs_data[cs][0])

logger.info("Reconstructed Chunk placement details")

# STart a thread to rebalance replicas every 70 seconds
thread = threading.Thread(target=rebalancing)
thread.start()

class MasterService(rpyc.Service):
	# Register a chunkserver
	def exposed_register(self,serverID,port):
		register_chunk_server(serverID,port)
	
	# Send the names of the files stored in GFS 
	def exposed_get_files(self):
		return list(fc_map.keys())

	# check if a file is stored in GFS
	def exposed_file_exists(self,filename):
		if filename in fc_map.keys():
			return True
		return False
		
	# get list of chunks for a given file and the server address which has them
	def exposed_chunk_info(self,filename):
		data=[]
		for chunk in fc_map[filename]:
			if valid_chunks[chunk]!=[]:
				data.append([chunk,valid_chunks[chunk][0]])
			else:
				return False,None
		return True,data
	
	# Remove a file from GFS	
	def exposed_delete_file(self,filename):
		# Assign it a hidden name and store it temporarily
		new_name="tmp"+str(uuid.uuid1().int)
		fc_map[new_name]=fc_map[filename]
		logger.info("Renamed the file to be deleted,"+filename+" to "+new_name)
		del fc_map[filename]
		persist()
		# Delete the file in a period of 50 seconds 
		thread = threading.Thread(target=lazy_delete_file, args=(new_name,))
		thread.start()
		return new_name
	
	# find servers to store a chunk and return a chunk ID to name the chunk	
	def exposed_find_chunk_space(self,filename):
		chunk_name=str(uuid.uuid1().int)+"v0"
		server_list=list(chunkservers.keys())
		free_server_list=free_server(server_list)[:3]
		valid_chunks[chunk_name]=[]
		if filename not in fc_map:
			fc_map[filename]=[]
		fc_map[filename].append(chunk_name)
		persist()
		for s in free_server_list:
			valid_chunks[chunk_name].append(s)
			chunkservers[s][1].append(chunk_name)
		logger.info("Created new chunk "+chunk_name+" for file "+filename)
		return chunk_name,free_server_list
	
	# Return the new version chunk ID and send server address which has the old version chunk	
	def exposed_append_chunk(self,filename,chunk):
		chunk_id,chunk_v=chunk.rsplit('v',1)
		chunk_v="v"+str(int(chunk_v)+1)
		new_chunk=chunk_id+chunk_v
		logger.info("Incrementing version of chunk "+chunk_id)
		fc_map[filename].remove(chunk)
		fc_map[filename].append(new_chunk)
		persist()
		valid_chunks[new_chunk]=valid_chunks[chunk]
		del valid_chunks[chunk]
		for s in valid_chunks[new_chunk]:
			chunkservers[s][1].remove(chunk)
			chunkservers[s][1].append(new_chunk)
		return new_chunk,valid_chunks[new_chunk]

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    t = ThreadedServer(MasterService, port=myPort)
    t.start()
