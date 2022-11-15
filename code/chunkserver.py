import rpyc
import sys
import os
import copy

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

myPort = int(sys.argv[1])
myID = sys.argv[2]


chunkserver_disk="//home//pes1ug19cs086//Desktop//PyGFS//chunkserver-disk//"
myDisk=os.path.join(chunkserver_disk, myID)
# Create disk space if not already available
if myID not in os.listdir(chunkserver_disk):
	os.mkdir(os.path.join(chunkserver_disk, myID))

# Store info of the chunks available in its Disk
chunks=[]
for chunk in os.listdir(myDisk):
	chunks.append(chunk)


class ChunkServerService(rpyc.Service):
	def exposed_chunk_list(self):
		return chunks
		
	# Make chunks present are up to date with master
	def exposed_refresh_chunks(self,valid_chunks):
		for chunk in chunks:
			if chunk not in valid_chunks:
				chunks.remove(chunk)
				chunk_path=os.path.join(myDisk,chunk)
				os.remove(chunk_path)
		return True
		
	# Used to pull a chunk from another Chunkserver
	def exposed_pull_chunk(self,chunk_ID,port):
		c = rpyc.connect("localhost", port)
		data=copy.deepcopy(c.root.send_chunk_data(chunk_ID))
		c.close()
		return self.exposed_create_chunk(chunk_ID,data)
		
	# Delete out of date or unwanted chunks
	def exposed_delete_chunk(self,chunk_ID):
		chunks.remove(chunk_ID)
		chunk_path=os.path.join(myDisk,chunk_ID)
		os.remove(chunk_path)
		return True
		
	# Create a new chunk for the data
	def exposed_create_chunk(self,chunk_ID,data):
		file_path=os.path.join(myDisk,chunk_ID)
		with open(file_path, 'w') as f:
			f.write(data)
		chunks.append(chunk_ID)
		return True
    		
    	# Used by chunkserver to pass data to be written from one to the other
	def exposed_recursive_write(self,chunk_ID,data,servers):
		self.exposed_create_chunk(chunk_ID,data)
		rem_servers=copy.deepcopy(servers)
		rem_servers.remove(myPort)
		if rem_servers!=[]:
			c = rpyc.connect("localhost", rem_servers[0])
			c.root.recursive_write(chunk_ID,data,rem_servers)
			c.close()
    		
    	# Find out available space in the chunk
	def exposed_chunk_space(self,chunk_ID):
		file_path=os.path.join(myDisk,chunk_ID)
		with open(file_path) as f:
			contents = f.read()
		return 100-len(contents)
    		
    	# Used to append data to an existing chunk
	def exposed_append_chunk(self,old_chunk,new_chunk,data):
		file_path=os.path.join(myDisk,old_chunk)
		with open(file_path, 'a') as f:
			f.write(data)
		chunks.remove(old_chunk)
		chunks.append(new_chunk)
		new_file_path=os.path.join(myDisk,new_chunk)
		os.rename(file_path,new_file_path)
		return True
    		
    	# Used to pass append data between chunkservers
	def exposed_recursive_append(self,old_chunk,new_chunk,data,servers):
		self.exposed_append_chunk(old_chunk,new_chunk,data)
		rem_servers=copy.deepcopy(servers)
		rem_servers.remove(myPort)
		if rem_servers!=[]:
			c = rpyc.connect("localhost", rem_servers[0])
			c.root.recursive_append(old_chunk,new_chunk,data,rem_servers)
			c.close()
    	# Send data present in a chunk
	def exposed_send_chunk_data(self,chunk_ID):
		file_path=os.path.join(myDisk,chunk_ID)
		with open(file_path) as f:
			contents = f.read()
		return contents
	
if __name__ == "__main__":
	from rpyc.utils.server import ThreadedServer
	t = ThreadedServer(ChunkServerService, port=myPort)
	t.start()
