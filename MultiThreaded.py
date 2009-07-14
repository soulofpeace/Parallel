import os, sys, datetime, logging, logging.handlers, errno, threading, Queue, time, random, math, gzip, re, hashlib
from xml.dom import minidom


readingJobs = Queue.Queue()
processingJobs = Queue.Queue()
writingJobs = Queue.Queue()
loggingJobs = Queue.Queue()
nameSet = set()

class dispatcher():
	maxReaderThreads =int(sys.argv[1])
	maxProcessorThreads = int(sys.argv[2])
	maxWriterThreads = int(sys.argv[3])



	def start(self):
		loggerThread(1,  dispatcher.maxReaderThreads + dispatcher.maxProcessorThreads+dispatcher.maxWriterThreads+1).start()
		self.readNameList(sys.argv[5])
		
		for i in range(dispatcher.maxReaderThreads):
			readerThread(i).start()
			
		for i in range(dispatcher.maxProcessorThreads):
			processorThread(i).start()
			
		for i in range(dispatcher.maxWriterThreads):
			writerThread(i).start()
			
		
		self.transverseDir(os.path.abspath(sys.argv[4]))
		loggingJobs.put(('dispatcher', loggerThread.INFO, 'All Done! Sending terminating signal to all reader thread!'))
			
		for i in range(dispatcher.maxReaderThreads):
			readingJobs.put(None)
			
		loggingJobs.put(None)
		
		
	
	def transverseDir(self, root):
		dirList = os.listdir(root)
		for file in dirList:
			filepath = os.path.join(root, file)
			if(os.path.isdir(filepath)):
				self.transverseDir(filepath)
			else:
				if filepath.endswith('gz'):
					#print 'dispatcher'+filepath
					readingJobs.put(filepath)
					
	def readNameList(self, nameFileName):
		nameFile = open(nameFileName, 'r')
		for name in nameFile:	
			name= name.strip()
			loggingJobs.put(('dispatcher', loggerThread.INFO, 'Name is '+name))
			nameSet.add(name)
		nameFile.close()



class loggerThread(threading.Thread):
	ERROR =1
	INFO= 2
	DEBUG =3
	
	def __init__(self, id, stopThreadCount):
		threading.Thread.__init__(self, name="logger-%d" % (id,))
		self.logger = logging.getLogger('parseSource_reader')
		hdlr = logging.handlers.TimedRotatingFileHandler('./logs/MultiThread.log', 'D', 1)
		formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
		hdlr.setFormatter(formatter)
		self.logger.addHandler(hdlr)
		self.logger.setLevel(logging.INFO)
		self.stopThreadCount = stopThreadCount
		
	def run(self):
		while 1:
			job = loggingJobs.get(True)
			if job is None:
				self.stopThreadCount = self.stopThreadCount - 1
				if(self.stopThreadCount==0):
					break
			else:
				sender, type, message = job
				if type ==loggerThread.ERROR:
					self.logger.error(sender+':::'+message)
				elif type == loggerThread.INFO:
					self.logger.info(sender+':::'+message)
				else:
					self.logger.debug(sender+':::'+message)
				



class readerThread(threading.Thread):
	
	def __init__(self, id):
		threading.Thread.__init__(self, name="reader-%d" % (id,))
		loggingJobs.put((self.getName(), loggerThread.INFO, self.getName()+' initialised'))
		
	def run(self):
		loggingJobs.put((self.getName(), loggerThread.INFO, self.getName()+' running'))
		while 1:
			job = readingJobs.get(True)
			if job is None:
				loggingJobs.put((self.getName(), loggerThread.INFO, 'received Terminating signal from dispatcher'))
				loggingJobs.put((self.getName(), loggerThread.INFO, 'sending Terminating signal to all processor'))
				loggingJobs.put(None)
				for i in range(dispatcher.maxProcessorThreads):
					processingJobs.put((self.getName(), None))
				break
			else:
				output = self.readFile(job)
				processingJobs.put((self.getName(), (job, output)))
				
	def readFile(self, fileName):
		f = gzip.open(fileName, 'rb')
		s=''
		for line in f:
			s = s+line
		f.close()
		return s		

class processorThread(threading.Thread ):
	stop = False

	def __init__(self, id):
		threading.Thread.__init__(self, name="processor-%d" % (id,))
		loggingJobs.put((self.getName(), loggerThread.INFO, self.getName()+' initialised'))
		self.readerStatus={}
		for i in range(dispatcher.maxReaderThreads):
			self.readerStatus['reader-'+str(i)]=(1, 0)
		

	def  run(self):
		loggingJobs.put((self.getName(), loggerThread.INFO, self.getName()+' running'))
		while 1:
			task= processingJobs.get(True)
			sender, job = task
			if job is None:
				if(processorThread.stop):
					loggingJobs.put((self.getName(), loggerThread.INFO, 'all reader threads stopped, preparing to stop'))
					loggingJobs.put(None)
					processorThread.stop = True
					for i in range(dispatcher.maxWriterThreads):
						writingJobs.put((self.getName(), None))
					break
				loggingJobs.put((self.getName(), loggerThread.INFO, 'received Terminating Signal'))
				if (self.readerStatus[sender][0])==1:
					loggingJobs.put((self.getName(), loggerThread.INFO, 'received Terminating Signal from '+ sender))
					self.readerStatus[sender]= (0, 0)
				else:
					loggingJobs.put((self.getName(), loggerThread.INFO, 'received duplicate Terminating Signal from '+ sender))
					receivedCount= self.readerStatus[sender][1]+1
					processingJobs.put((sender, job))
					self.readerStatus[sender] =(self.readerStatus[sender][0], receivedCount)
					sleeptime = receivedCount*random.random()
					loggingJobs.put((self.getName(), loggerThread.INFO, 'Sleeping for '+str(sleeptime)))
					time.sleep(sleeptime)
				count=0
				for val in self.readerStatus.values():
					if val[0] ==0:
						count = count +1
				if count==dispatcher.maxReaderThreads:
					loggingJobs.put((self.getName(), loggerThread.INFO, 'all reader threads stopped, preparing to stop'))
					loggingJobs.put(None)
					processorThread.stop = True
					for i in range(dispatcher.maxWriterThreads):
						writingJobs.put((self.getName(), None))
					break
			else:
				#print self.getName()+':::'+job
				fileName, xml = job
				name = self.check(xml)
				if(name is None):
					loggingJobs.put((self.getName(), loggerThread.INFO, 'No listed name found in'+ fileName))
					print fileName
					continue
				else:
					loggingJobs.put((self.getName(), loggerThread.INFO, 'listed name found in'+ fileName))
					writingJobs.put((self.getName(), (fileName, name, hashlib.sha224(xml).hexdigest(), xml)))
				
	def check(self ,xml):
		regex = r"""<element><!\[CDATA\[(?P<element>.*?)\]\]></element>"""
		p = re.compile(regex, re.UNICODE|re.IGNORECASE)
		#print xml
		m = p.search(xml)
		if m:
			loggingJobs.put((self.getName(), loggerThread.INFO, 'found Field element'))
			name = m.group('element')
			loggingJobs.put((self.getName(), loggerThread.INFO, 'name found is '+name))
			if(name in nameSet):
				return name
			else:
				return  None



class writerThread(threading.Thread):
	stop = False
	def __init__(self, id):
		threading.Thread.__init__(self, name="writer-%d" % (id,))
		loggingJobs.put((self.getName(), loggerThread.INFO, self.getName()+' initialised'))
		self.processorStatus={}
		for i in range(dispatcher.maxProcessorThreads):
			self.processorStatus['processor-'+str(i)]=(1, 0)
		

	def  run(self):
		loggingJobs.put((self.getName(), loggerThread.INFO, self.getName()+' running'))
		while 1:
			sender, job = writingJobs.get(True)
			if job is None:
				if(writerThread.stop):
					loggingJobs.put((self.getName(), loggerThread.INFO, 'all processor thread stop preparing to stop'))
					loggingJobs.put(None)
					writerThread.stop = True
					break

				loggingJobs.put((self.getName(), loggerThread.INFO, 'received Terminating Signal'))
				if(self.processorStatus[sender][0]==1):
					loggingJobs.put((self.getName(), loggerThread.INFO, 'received Terminating Signal from '+sender))
					self.processorStatus[sender]=(0, 0)
				else:
					loggingJobs.put((self.getName(), loggerThread.INFO, 'received duplicate Terminating Signal from '+sender))
					receivedCount=self.processorStatus[sender][1]+1
					writingJobs.put((sender, job))
					self.processorStatus[sender]=(self.processorStatus[sender][0], receivedCount)
					sleeptime =receivedCount*random.random()
					loggingJobs.put((self.getName(), loggerThread.INFO, 'sleeping for '+str(sleeptime)))
					time.sleep(sleeptime)
				count=0
				for val in self.processorStatus.values():
					if val[0] ==0:
						count = count +1
				if(count==dispatcher.maxProcessorThreads):
					loggingJobs.put((self.getName(), loggerThread.INFO, 'all processor thread stop preparing to stop'))
					loggingJobs.put(None)
					writerThread.stop = True
					break
			else:
				
				fileName, dir, file, output = job
				loggingJobs.put((self.getName(), loggerThread.INFO, 'Received File ' +fileName))
				self.writeToFile(dir, file, output)
				print fileName
				
				
	def writeToFile(self, dir, file, output):
		workingdir = os.getcwd()+'/data/'
		loggingJobs.put((self.getName(), loggerThread.INFO, 'working dir is ' +workingdir))
		destinationDir = os.path.join(workingdir, dir)
		if(not os.path.exists(destinationDir)):
			os.mkdir(destinationDir)
		outfileName=os.path.join(destinationDir, file)
		loggingJobs.put((self.getName(), loggerThread.INFO, 'output file name is ' +outfileName))
		outfile=open(outfileName, 'w')
		outfile.write(output)
		outfile.close()


		
		

def main():
	d = dispatcher()
	d.start()

if __name__=='__main__':
	main()
