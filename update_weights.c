#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>

typedef struct {
    double cpu_coef;
    double memory_coef;
} Coeficients;

typedef struct {
    int cpu_free;
    int mem_free;
} Metrics;

typedef struct {
    const char *name;
    const char *community;
    int weight;
} Host;

struct main_thread_info { /* Used as argument to run() */
    int periodicity;
    int num_servers;
    Coeficients *coeficients;
};

struct server_thread_info { /* Used as argument to server_run() */
    int id;    
    Coeficients *coeficients;
};

void *run(void *arg);
void *server_run(void *arg);
int determineNewWeight(Host *server, Coeficients *coeficients);
int getFreeCPU(int version, const char *community, const char *name_ip);
int getFreeMem(int version, const char *community, const char *name_ip);
FILE *exec_nmap(const char *name_ip);
FILE *exec_snmp(char *type, int version, const char *community, const char *name_ip, char *OID);
FILE *exec_command(char *command);

static Host *servers;

int main(int argc, char **argv)
{
    pthread_t thread;
    int ret, i;
    struct main_thread_info *tinfo;

    tinfo = malloc(sizeof(struct main_thread_info));
    tinfo->periodicity = 5;
    tinfo->num_servers = 2;

    tinfo->coeficients = (Coeficients *) malloc(sizeof(Coeficients));
    tinfo->coeficients->cpu_coef = 0.1;
    tinfo->coeficients->memory_coef = 0.9;

    servers = (Host *) malloc(tinfo->num_servers * sizeof(Host));
    servers[0].name = "192.168.56.102";
    servers[0].community = "public";  
    servers[0].weight = 1;  
    servers[1].name = "192.168.56.103";
    servers[1].community = "public";
    servers[1].weight = 1;  

    ret = pthread_create(&thread, NULL, run, (void*) tinfo);

    pthread_join(thread, NULL);

    printf("Thread returns: %d\n", ret);

    free(tinfo->coeficients);
    free(tinfo);

    return (EXIT_SUCCESS);
}

void *run(void *arg) {
    int i, ret, cnt = 1;
    unsigned int min;
    int *old_weight;
    pthread_t *server_threads;

    struct main_thread_info *tinfo = (struct main_thread_info *) arg;
    struct server_thread_info *sinfo;

    int num_servers = tinfo->num_servers;

    old_weight = (int *) malloc(num_servers * sizeof(int));
    server_threads = malloc(num_servers * sizeof (pthread_t));
    sinfo = (struct server_thread_info *) malloc(num_servers * sizeof(struct server_thread_info));

    for (i = 0; i < num_servers; i++) {
      old_weight[i] = 1;
    }

    printf("Main thread started!\n");

    while (1) {

        printf("Data collection %d!\n", cnt++);

        for (i = 0; i < num_servers; i++) {

            sinfo[i].id = i;
            sinfo[i].coeficients = (Coeficients *) malloc(sizeof (Coeficients));
            sinfo[i].coeficients = tinfo->coeficients;

            ret = pthread_create(&server_threads[i], NULL, server_run, (void*) &(sinfo[i]));
        }

        for (i = 0; i < num_servers; i++) {
            pthread_join(server_threads[i], NULL);
        }

        min = UINT_MAX;

        for (i = 0; i < num_servers; i++) {
printf("Server %s determined weight = %d\n", servers[i].name, servers[i].weight);
            if((servers[i].weight > 0) && (servers[i].weight < min))
              min = servers[i].weight;
        }

        for (i = 0; i < num_servers; i++) {
            servers[i].weight = old_weight[i] + (int) ((double) servers[i].weight/min + 0.5);
            servers[i].weight = (int) ((double) servers[i].weight / 2 + 0.5);
            printf("Server %s new weight = %d\n", servers[i].name, servers[i].weight);
        }


        sleep(tinfo->periodicity);
    }

    free(server_threads);
}

void *server_run(void *arg) {

    struct server_thread_info *sinfo = (struct server_thread_info *) arg;
    int weight, id;

    id = sinfo->id;

    printf("Thread Server %s started!\n", servers[id].name);

    servers[id].weight = determineNewWeight(&servers[id], sinfo->coeficients);

    printf("Thread Server %s ended!\n", servers[id].name);
}

int determineNewWeight(Host *server, Coeficients *coeficients) {
    //printf("Server %s calculating new weight!\n", server->name);

    int cpu_free, num_conn, mem_free;
    int weight = 0;

    int version = 1;
    int interface_id = 0;

    if(http_port_open(server->name))
    {
        cpu_free = getFreeCPU(version, server->community, server->name);
        printf("Free %% CPU for server %s = %d\n", server->name, cpu_free);
        if (cpu_free == -1)
            cpu_free = 0;

        mem_free = getFreeMem(version, server->community, server->name);
        printf("Free %% Memory for server %s = %d\n", server->name, mem_free);
        if (mem_free == -1)
            mem_free = 0;

        weight = (int) ((double) (coeficients->cpu_coef * cpu_free + coeficients->memory_coef * mem_free) + 0.5);
    }

    return weight;
}

int http_port_open(const char *name_ip) {
    FILE *fp;
    char output[1024];
    char *result;

    fp = exec_nmap(name_ip);

    while (fgets(output, sizeof (output) - 1, fp) != NULL) {
        if(strstr(output, "80/tcp") != NULL) {
            result = (char *) strtok(output, " ");
            if (result != NULL) {
                result = (char *) strtok(NULL, " ");
            }
            break;
        } 
    }

    if(result != NULL && strcmp(result, "open") == 0)
        return 1;
    return 0;
}

int getFreeCPU(int version, const char *community, const char *name_ip) {
    FILE *fp;
    char output[1024];
    char *OID = "1.3.6.1.2.1.25.3.3.1.2";
    int counter = 0;
    char *result = NULL, *res;
    int int_res = -1;

    fp = exec_snmp("walk", version, community, name_ip, OID);

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
              int_res = atoi(result);
            else
              int_res += atoi(result);
        }
    }

    if (int_res > -1 && counter > 0) {
        int_res = (int) ((double) int_res/counter + 0.5);
        int_res = 100 - int_res;
    }

    /* close */
    pclose(fp);

    return int_res;
}

int getFreeMem(int version, const char *community, const char *name_ip) {
    FILE *fp;
    char output[1024];
    char *result = NULL, *res;
    int int_free = -1, int_total = -1;
    int counter = 0;

    fp = exec_snmp("get", version, community, name_ip, "1.3.6.1.4.1.2021.4.11.0 1.3.6.1.4.1.2021.4.5.0");

    while (fgets(output, sizeof (output) - 1, fp) != NULL) {
        if (strstr(output, "Timeout") == NULL) {
            result = (char *) strtok(output, " ");
            while (result != NULL) {
                res = result;
                result = (char *) strtok(NULL, " ");
            }
            result = (char *) strtok(res, "\r");

            if (strstr(output, "1.3.6.1.4.1.2021.4.11.0") != NULL)
              int_free = atoi(result);
            else if (strstr(output, "1.3.6.1.4.1.2021.4.5.0") != NULL)
              int_total = atoi(result);
        }
    }

    pclose(fp);

    if(int_free > -1 && int_total > 0)
        return (int) ((((double) int_free/int_total) * 100) + 0.5);
    else
        return -1;
}

/*
int getEstablishedConnections(int version, char *community, char *name_ip, int interface_id) {
    FILE *fp;
    char output[1024];
    char OID[128];
    char *result = NULL, *res;
    int int_res = -1;

    sprintf(OID, "%s%d", "1.3.6.1.2.1.6.9.", interface_id);

    fp = exec_command("get", version, community, name_ip, OID);

    if (fgets(output, sizeof (output) - 1, fp) != NULL) {
        if (strstr("Timeout", output) == NULL) {

            result = (char *) strtok(output, " ");
            while (result != NULL) {
                res = result;
                result = (char *) strtok(NULL, " ");
            }

            result = (char *) strtok(res, "\r");

            int_res = atoi(result);
        }
    }

    pclose(fp);

    return int_res;
}*/

FILE *exec_nmap(const char *name_ip) {

    char command[1024];

    sprintf(command, "%s %s", "nmap -p 80", name_ip);

    return exec_command(command);
}

FILE *exec_snmp(char *type, int version, const char *community, const char *name_ip, char *OID) {

    char command[1024];

    sprintf(command, "%s%s %s %d %s %s %s %s", "snmp", type, "-O enU -v", version, "-c", community, name_ip, OID);

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

