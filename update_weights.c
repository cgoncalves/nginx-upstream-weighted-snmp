#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>

typedef struct {
    double cpu;
    double memory;
    double connections;
} Coeficients;

typedef struct {
    unsigned int cpu_free;
    unsigned int mem_free;
    unsigned int connections;
} Metrics;

typedef struct {
    const char *name;
    const char *community;
    Metrics metrics;
    unsigned int old_weight;
    unsigned int weight;
} Host;

struct main_thread_info { /* Used as argument to run() */
    int periodicity;
    int num_servers;
};

struct server_thread_info { /* Used as argument to server_run() */
    int id;
};

void *run(void *arg);
void *server_run(void *arg);
void determineNewWeights(int num_servers);
void getMetrics(int id);
FILE *exec_wget(const char *name_ip);
FILE *exec_snmp(char *type, int version, const char *community, const char *name_ip, char *OID);
FILE *exec_command(char *command);

static Host *servers;
static Coeficients *coefs;

int main(int argc, char **argv)
{
    pthread_t thread;
    int ret, i;
    struct main_thread_info *tinfo;

    tinfo = malloc(sizeof(struct main_thread_info));
    tinfo->periodicity = 5;
    tinfo->num_servers = 2;

    coefs = (Coeficients *) malloc(sizeof(Coeficients));
    coefs->cpu = 0.4;
    coefs->memory = 0.4;
    coefs->connections = 0.2;

    servers = (Host *) malloc(tinfo->num_servers * sizeof(Host));
    servers[0].name = "192.168.56.102";
    servers[0].community = "public";  
    servers[0].old_weight = 1;  
    servers[1].name = "192.168.56.103";
    servers[1].community = "public";
    servers[1].old_weight = 1;

    ret = pthread_create(&thread, NULL, run, (void*) tinfo);

    pthread_join(thread, NULL);

    printf("Thread returns: %d\n", ret);

    free(tinfo);
    free(coefs);
    free(servers);

    return (EXIT_SUCCESS);
}

void *run(void *arg) {
    int i, ret, cnt = 1, new_weight;
    pthread_t *server_threads;

    struct main_thread_info *tinfo = (struct main_thread_info *) arg;
    struct server_thread_info *sinfo;

    int num_servers = tinfo->num_servers;

    server_threads = malloc(num_servers * sizeof (pthread_t));
    sinfo = (struct server_thread_info *) malloc(num_servers * sizeof(struct server_thread_info));

    printf("Main thread started!\n");

    while (1) {
        printf("Data collection %d!\n", cnt++);

        for (i = 0; i < num_servers; i++) {
            sinfo[i].id = i;
            ret = pthread_create(&server_threads[i], NULL, server_run, (void*) &(sinfo[i]));
        }

        for (i = 0; i < num_servers; i++) {
            pthread_join(server_threads[i], NULL);
        }

        determineNewWeights(num_servers);

        sleep(tinfo->periodicity);
    }

    free(server_threads);
}

void *server_run(void *arg) {

    struct server_thread_info *sinfo = (struct server_thread_info *) arg;
    int cpu_free, num_conn, mem_free;
    int version = 1, id;
    unsigned int weight = 0;

    id = sinfo->id;

    printf("Thread Server %s started!\n", servers[id].name);

    if(http_port_open(servers[id].name))
    {
        getMetrics(id);
        printf("Free %% CPU for server %s = %d\n", servers[id].name, servers[id].metrics.cpu_free);
        printf("Free %% Memory for server %s = %d\n", servers[id].name, servers[id].metrics.mem_free);
        printf("Number of connections for server %s = %d\n", servers[id].name, servers[id].metrics.connections);

        if (servers[id].metrics.cpu_free == -1)
            servers[id].metrics.cpu_free = 0;

        if (servers[id].metrics.mem_free == -1)
            servers[id].metrics.mem_free = 0;
    }

    printf("Thread Server %s ended!\n", servers[id].name);
}

void determineNewWeights(int num_servers) {

    int i;
    unsigned int min, max;

    max = 0;
    min = UINT_MAX;

    for (i = 0; i < num_servers; i++) {
        if((servers[i].metrics.connections > 0) && (servers[i].metrics.connections > max))
          max = servers[i].metrics.connections;
    }

    for (i = 0; i < num_servers; i++) {

        if (servers[i].metrics.connections == -1)
            servers[i].metrics.connections = max;

        if(max > 0)
          servers[i].metrics.connections = (int) ((1 - (double) servers[i].metrics.connections/max) * 100 + 0.5);

        printf("Server %s determined weight for connections = %d\n", servers[i].name, servers[i].metrics.connections);

        servers[i].weight = (int) ((double) (coefs->cpu * servers[i].metrics.cpu_free + coefs->memory * servers[i].metrics.mem_free + coefs->connections * servers[i].metrics.connections) + 0.5);

        printf("Server %s determined weight = %d\n", servers[i].name, servers[i].weight);

        if((servers[i].weight > 0) && (servers[i].weight < min))
          min = servers[i].weight;
    }

    for (i = 0; i < num_servers; i++) {
        if(min > 0)
          servers[i].weight = (int) ((double) servers[i].weight/min + 0.5);

        if(servers[i].weight > 0)
        {
          servers[i].weight = servers[i].old_weight + servers[i].weight;
          servers[i].weight = (int) ((double) servers[i].weight / 2 + 0.5);
        }
        else
          servers[i].weight = 0;
        
        servers[i].old_weight = servers[i].weight;

        printf("Server %s new weight = %d\n", servers[i].name, servers[i].weight);
    }
}

int http_port_open(const char *name_ip) {
    FILE *fp;
    char output[1024], *result;
    int counter = 0;

    fp = exec_wget(name_ip);

    while (fgets(output, sizeof (output) - 1, fp) != NULL) {
        counter++;
        if(counter == 2 && strstr(output, "Connecting") != NULL && strstr(output, "connected") != NULL) {
            return 1;
        } 
        if(counter > 2)
          break;
    }
    return 0;
}

void getMetrics(int id) {
    FILE *fp;
    char output[1024], OIDs[1024];;
    int counter = 0;
    char *result = NULL, *res;
    int cpu_free = -1, mem_free = -1, mem_total = -1, connections = -1;

    /* Free CPU percentage */

    fp = exec_snmp("walk", 1, servers[id].community, servers[id].name, "1.3.6.1.2.1.25.3.3.1.2");

    while (fgets(output, sizeof (output) - 1, fp) != NULL) {
        if (strstr(output, "Timeout") == NULL) {
            counter++;
            result = (char *) strtok(output, " ");
            while (result != NULL) {
                res = result;
                result = (char *) strtok(NULL, " ");
            }
            result = (char *) strtok(res, "\r");
            if(counter == 1)
              cpu_free = atoi(result);
            else
              cpu_free += atoi(result);
        }
    }
    pclose(fp);

    if (cpu_free > -1 && counter > 0) {
        cpu_free = (int) ((double) cpu_free/counter + 0.5);
        cpu_free = 100 - cpu_free;
    }
    servers[id].metrics.cpu_free = cpu_free;

    /* Free Memory percentage, established TCP connections */
    fp = exec_snmp("get", 1, servers[id].community, servers[id].name, "1.3.6.1.4.1.2021.4.11.0 1.3.6.1.4.1.2021.4.5.0 1.3.6.1.2.1.6.9.0");

    while (fgets(output, sizeof (output) - 1, fp) != NULL) {
        if (strstr(output, "Timeout") == NULL) {
            result = (char *) strtok(output, " ");
            while (result != NULL) {
                res = result;
                result = (char *) strtok(NULL, " ");
            }
            result = (char *) strtok(res, "\r");
            if (strstr(output, "1.3.6.1.4.1.2021.4.11.0") != NULL)
              mem_free = atoi(result);
            else if (strstr(output, "1.3.6.1.4.1.2021.4.5.0") != NULL)
              mem_total = atoi(result);
            else if (strstr(output, "1.3.6.1.2.1.6.9") != NULL)
              connections = atoi(result);
        }
    }
    pclose(fp);

    if(mem_free > -1 && mem_total > 0)
        servers[id].metrics.mem_free = (int) ((((double) mem_free/mem_total) * 100) + 0.5);
    else
        servers[id].metrics.mem_free = -1;

    servers[id].metrics.connections = connections;
}

FILE *exec_wget(const char *name_ip) {

    char command[1024];

    sprintf(command, "wget -O - %s 2>&1", name_ip);

    return exec_command(command);
}

FILE *exec_snmp(char *type, int version, const char *community, const char *name_ip, char *OID) {

    char command[1024];

    sprintf(command, "snmp%s -O enU -v %d -c %s %s %s", type, version, community, name_ip, OID);

    return exec_command(command);
}

FILE *exec_command(char *command) {
    FILE *fp;

    /* Open the command for reading. */
    fp = popen(command, "r");
    if (fp == NULL) {
        printf("Failed to run command\n");
        exit;
    }

    return fp;
}

