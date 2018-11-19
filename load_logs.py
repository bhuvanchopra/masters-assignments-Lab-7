import sys, os, gzip, re, uuid
import datetime as dt
from cassandra.cluster import Cluster, BatchStatement, ConsistencyLevel

def main(input_dir, keyspace, output_table):

    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(keyspace)

    linesep = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

    insert_nasalogs = session.prepare("INSERT INTO nasalogs (id, host, datetime, path, bytes) VALUES (?, ?, ?, ?, ?)")
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    
    session.execute("TRUNCATE " + keyspace + '.' + output_table + ';')    
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt') as logfile:
            count = 0
            for line in logfile:
                linesplit = linesep.split(line)
                if len(linesplit) > 4:
                    batch.add(insert_nasalogs, (uuid.uuid1(), linesplit[1], \
                    dt.datetime.strptime(linesplit[2], '%d/%b/%Y:%H:%M:%S'), linesplit[3], int(linesplit[4])))

                    count += 1

                    if count > 300:
                        session.execute(batch)
                        batch.clear()
                        count = 0

            session.execute(batch)

if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    output_table = sys.argv[3]
    main(input_dir, keyspace, output_table)
