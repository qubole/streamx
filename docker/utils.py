import sys

def print_help():
  print """ The following properties must be configured using env variables
  CONNECT_BOOTSTRAP_SERVERS
  CONNECT_AWS_ACCESS_KEY
  CONNECT_AWS_SECRET_KEY
  Cmd : docker run -d --env CONNECT_BOOTSTRAP_SERVERS=public_dns:9092 --env CONNECT_AWS_ACCESS_KEY=xxxxx --env CONNECT_AWS_SECRET_KEY=yyyy --env CONNECT_CLUSTER_ON_ROLES=True/False qubole/streamx"""

def check_for_required_configs(confs):
  if len(confs) == 0:
    print_help()
    sys.exit(1)

  required_configs = ["CONNECT_BOOTSTRAP_SERVERS", "CONNECT_AWS_ACCESS_KEY", "CONNECT_AWS_SECRET_KEY", "CONNECT_CLUSTER_ON_ROLES"]
  for x in required_configs:
    if x not in confs.keys():
       print x +" is required"
       sys.exit(1)

  cluster_on_roles = confs['CONNECT_CLUSTER_ON_ROLES']
  if cluster_on_roles == "False":
      if ((not confs['CONNECT_AWS_ACCESS_KEY'].strip()) or (not confs['CONNECT_AWS_SECRET_KEY'].strip())):
          print "AWS ACCESS and SECRET keys are required when not on Roles."
          sys.exit(1)

def override_connect_configs(confs):
  connect_file = "/usr/local/streamx/config/connect-distributed.properties"
  connect_override_file = "/usr/local/streamx/config/connect-distributed-override.properties"

  with open(connect_file) as f:
    connect_conf = f.read().splitlines()

  # env_vars will be of format CONNECT_BOOTSTRAP_SERVERS corresponding to bootstrap.servers. 
  # This code converts the env_vars into later format
  for k,v in confs.items():
    #remove CONNECT_ prefix
    k = k[8:]
    key = k.lower().replace("_",".") + "="
      
    # override connect_conf with env_vars
    for i in range(0, len(connect_conf)):
      if connect_conf[i].startswith(key):
        connect_conf[i] = key + v

  with open(connect_override_file,'w') as f:
    for line in connect_conf:
      f.write(line+"\n")

def override_hadoop_configs(confs):
  hadoop_conf_file = "/usr/local/streamx/config/hadoop-conf/hdfs-site.xml"
  cluster_on_roles = confs["CONNECT_CLUSTER_ON_ROLES"]
  import subprocess
  cmd='sed -i "s:IS_CLUSTER_ON_ROLES:' + cluster_on_roles + ':g" /usr/local/streamx/config/hadoop-conf/hdfs-site.xml'
  subprocess.Popen(cmd, shell=True).wait()

  if cluster_on_roles == "False":
    access_key = confs["CONNECT_AWS_ACCESS_KEY"]
    secret_key = confs["CONNECT_AWS_SECRET_KEY"]

    cmd1='sed -i "s:SECRET_KEY_HERE:' + secret_key + ':g" /usr/local/streamx/config/hadoop-conf/hdfs-site.xml'
    cmd2='sed -i "s:ACCESS_KEY_HERE:' + access_key + ':g" /usr/local/streamx/config/hadoop-conf/hdfs-site.xml'
    subprocess.Popen(cmd1, shell=True).wait()
    subprocess.Popen(cmd2, shell=True).wait()

def main():
  confs = {}
  for x in range(1,len(sys.argv)):
    if sys.argv[x].startswith("CONNECT"):
      (k, v) = sys.argv[x].split("=", 1)
      confs[k] = v
    else:
      print "Ignoring " + x + " as it does not start with CONNECT_"

  check_for_required_configs(confs)
  override_connect_configs(confs)
  override_hadoop_configs(confs)

if __name__ == "__main__":
  main()

