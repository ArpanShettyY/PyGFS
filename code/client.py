import rpyc
import os
import copy

rpyc.core.protocol.DEFAULT_CONFIG['allow_pickle'] = True

myDisk="//home//pes1ug19cs086//Desktop//PyGFS//client-disk"

# Contact the GFS Script to get masters address(port)
def get_master_port():
	c = rpyc.connect("localhost", 18861)
	port=copy.deepcopy(c.root.get_master_details())
	c.close()
	return port

# Contact the GFS Script to get chunkservers address(port)
def get_chunkserver_info():
	c = rpyc.connect("localhost", 18861)
	chunkservers=copy.deepcopy(c.root.get_chunkserver_details())
	c.close()
	return chunkservers

# Combine chunks of a file to recreate the complete data and display it
def read_file(filename):
	master_port=get_master_port()
	c = rpyc.connect("localhost", master_port)
	# Check if file exists before attempting to read
	if c.root.file_exists(filename):
		av,chunks=copy.deepcopy(c.root.chunk_info(filename))
		# Check if each of the file's chunk is available on atleast one chunkserver
		if av:
			chunkservers=get_chunkserver_info()
			for chunk in chunks:
				cs= rpyc.connect("localhost", chunkservers[chunk[1]][0])
				print(cs.root.send_chunk_data(chunk[0]),end="")
				cs.close()
		else:
			print("Cannot read full file at the moment")
		
	else:
		print("File does not exist")
	c.close()
	
# Divide the data into chunks and store it in Chunkservers
def write_file(filename,data,ap=False):
	length=len(data)
	# Find number of chunks required
	num_chunks=int(length/100)
	master_port=get_master_port()
	c = rpyc.connect("localhost", master_port)	
	if c.root.file_exists(filename) and not(ap):
		print("File already exists")
	else:
		chunkservers=get_chunkserver_info()
		# Adding another chunk for left off data
		if length%100!=0:
			num_chunks+=1
			
		# Send the correct amount of data to the chunkservers the master had replied with
		for i in range(num_chunks):
			s=i*100
			e=min(length,(i+1)*100)
			chunk_data=data[s:e]
			chunk_id,cservers=copy.deepcopy(c.root.find_chunk_space(filename))
			server_ports=[]
			for s in cservers:
				server_ports.append(chunkservers[s][0])
			cs= rpyc.connect("localhost", server_ports[0])
			cs.root.recursive_write(chunk_id,chunk_data,server_ports)
			cs.close()
			
	c.close()
	
# Append to an existing file, create new chunks if necessary
def append_file(filename,data):
	master_port=get_master_port()
	c = rpyc.connect("localhost", master_port)
	# Confirm that the file exists before appending
	if c.root.file_exists(filename):
		ac,info=copy.deepcopy(c.root.chunk_info(filename))
		# If the chunks are present in atleast one chunkserver
		if ac==False:
			print("Servers down append currently not possible.Retry in a while")
		else:
			last_chunk,server=info[-1]
			chunkservers=get_chunkserver_info()
			cs= rpyc.connect("localhost", chunkservers[server][0])
			rem=copy.deepcopy(cs.root.chunk_space(last_chunk))
			cs.close()
			
			l=len(data)
			
			new_chunk,cservers=copy.deepcopy(c.root.append_chunk(filename,last_chunk))
			server_ports=[]
			for s in cservers:
				server_ports.append(chunkservers[s][0])
			# If the data can fit within previously present chunk
			if l<=rem:
				cs= rpyc.connect("localhost", server_ports[0])
				cs.root.recursive_append(last_chunk,new_chunk,data,server_ports)
				cs.close()
			# If new chunks are required to store data
			else:
				cs= rpyc.connect("localhost", server_ports[0])
				cs.root.recursive_append(last_chunk,new_chunk,data[:rem],server_ports)
				cs.close()
				write_file(filename,data[rem:],True)
			
			print("Appended to file successfully")
	else:
		print("File does not exist")
	c.close()
	
# Removing a file from GFS
def delete_file(filename):
	master_port=get_master_port()
	c = rpyc.connect("localhost", master_port)
	if c.root.file_exists(filename):
		new_name=copy.deepcopy(c.root.delete_file(filename))
		print("File will be accessible temporarily with name "+new_name+" before it is permanently deleted")
	else:
		print("File does not exist")
	c.close()
	
# A list of Files store in GFS	
def list_files():
	master_port=get_master_port()
	c = rpyc.connect("localhost", master_port)
	files=copy.deepcopy(c.root.get_files())
	c.close()
	return files
	
while(True):
	print("Choose an option:\n1.Read a file\n2.Write a new file\n3.Append to an existing file\n4.Delete a File\n5.List all files\n6.Quit")
	op=int(input())
	if op==1:
		print("Enter the name of the file you want to read:")
		filename=input()
		read_file(filename)
	elif op==2:
		print("Enter the name of the file you want to store:")
		filename=input()
		filepath=os.path.join(myDisk, filename)
		if os.path.exists(filepath):
			with open(filepath) as f:
				data = f.read()
			write_file(filename,data)
			print("File successfully written to GFS")
		else:
			print("File does not exist")
	elif op==3:
		print("Enter the name of the file you want to append to:")
		filename=input()
		print("Enter the data to append:")
		data=input()
		append_file(filename,data)
	elif op==4:
		print("Enter the name of the file you want to delete:")
		filename=input()
		delete_file(filename)
	elif op==5:
		files=list_files()
		if len(files)==0:
			print("<EMPTY>")
		else:
			for f in files:
				print(f)
	elif op==6:
		break
	else:
		print("Invalid Option")
	print("\n\n\n")
	
