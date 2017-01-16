import sys

if __name__ == "__main__":
	with open(sys.argv[1]) as input:
		with open(sys.argv[2], "w") as out:
			#write 20 MB of lines
			out.writelines(input.readlines(20000000))

	
