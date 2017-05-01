import sys
import subprocess as sp
import time
import socket

class ClientInterface:
	"""
	The interface talks to A client process through command line interface.
	"""
	def __init__(self, args, debug_mode=True):
		self.cli = sp.Popen(args, 
			stdout = sp.PIPE,
			stdin = sp.PIPE)
		self.debug = debug_mode

	"""
	input should be the line to input, without the '\n'
	"""
	def write(self, input):
		if self.debug:
			print "\twriting to stdin: \"{}\"".format(input)
		
		self.cli.stdin.write(input + '\n')
		
		if self.debug:
			print "\twrote to stdin"

	"""
	read a line from stdout, return the line without the '\n'
	"""
	def read(self):
		if self.debug:
			print "\treading from stdout"

		output = self.cli.stdout.readline()

		if self.debug:
			print "\tread from stdout: \"{}\"".format(output[:-1])

		return output[:-1]

	"""
	write input+'\n' to stdin, then read a line from stdout,
	return the line without '\n'
	"""
	def operation(self, input, expected=None):
		self.write(input)
		output = self.read()
		if expected is not None:
			assert output==expected, ("Test failed.\n"+
									"when we type: \"" + input +
									"\", expect output: \"" + expected +
									"\", but got :\"" + output + "\"")
		return output

def main():
	host_name = socket.gethostname()
	CAT = {"sp17-cs425-g07-07.cs.illinois.edu": "A",
           "sp17-cs425-g07-08.cs.illinois.edu": "B",
           "sp17-cs425-g07-09.cs.illinois.edu": "C"}
	Server = CAT[host_name]
	
	client = ClientInterface(args=sys.argv[1:], debug_mode=False)
	print "When your client process is ready, press Enter to start testing"
	sys.stdin.readline()
	# print "Evaluation starts!"
	time_record = []
	for _ in range(9):
		begin_t = time.time()

		client.operation("BEGIN", expected="OK")
		for _ in range(49):
			client.operation("SET "+Server+".course_name 425/428", expected="OK")
			client.operation("GET "+Server+".course_name", expected=Server+".course_name = 425/428")
		client.operation("COMMIT", expected="COMMIT OK")

		end_t = time.time()
		time_record.append((end_t - begin_t)*1000)
	
	print "each 100 opeartions: ms"
	print time_record

if __name__ == '__main__':
	main()
