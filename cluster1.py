import subprocess


class Cluster:

    PORT_NO = 8001
    PORT_NO = 8002
    PORT_NO = 8003

    def __init__(self):

        # start server 1 - Each of these should be a process

        process1 = subprocess.run(['python3', 'server1.py', '-port', '8001', '-cluster', '1'], capture_output=True, text=True)
        # process = subprocess.Popen(['python3', 'server1.py', '-port', '8001', '-cluster', '1'], stdout=subprocess.PIPE)  



        # while True:

        #     output = process.stdout.readline().decode('utf-8').rstrip()  

        #     if output:

        #         print(output)  

        #     if process.poll() is not None:  

        #         break  
            
       
       
        if process1.returncode == 0:
            print("Command executed successfully:")
            print(process1.stdout)
        else:
            print(f"Error executing command: {process1.stderr}")

        # start server 2
        process2 = subprocess.run(['python3', 'server2.py', '-port', '8001', '-cluster', '1'], capture_output=True, text=True)
        # process = subprocess.Popen(['python3', 'server1.py', '-port', '8001', '-cluster', '1'], stdout=subprocess.PIPE)  



        # while True:

        #     output = process.stdout.readline().decode('utf-8').rstrip()  

        #     if output:

        #         print(output)  

        #     if process.poll() is not None:  

        #         break  
            
       
       
        if process2.returncode == 0:
            print("Command executed successfully:")
            print(process2.stdout)
        else:
            print(f"Error executing command: {process2.stderr}")

        


        # start server 3
        pass





if __name__ == "__main__":

    cluster = Cluster()


