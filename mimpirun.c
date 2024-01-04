/**
 * This file is for implementation of mimpirun program.
 * */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include "channel.h"

#include "mimpi_common.h"

int main(int argc, char *argv[]) {
    long n = strtol(argv[1], NULL, 0);
    char *prog = argv[2];
    char* args[argc - 1];
    pid_t pid;
    for (int i = 2; i < argc; ++i) {
        args[i - 2] = argv[i];
    }
    args[argc - 2] = NULL;
    int fd[2];
    int channels_point_point[n][n][2];
    int channels_group[n][n][2];

    char num[20];
    sprintf(num, "%ld", n);
    ASSERT_SYS_OK(setenv("MIMPI_SIZE", num, 0));

    for (int i = 0; i < n; ++i) {
        if (group_num(i, MIMPI_Father) >= 0) {
            ASSERT_SYS_OK(channel(fd));
            channels_group[i][group_num(i, MIMPI_Father)][0] = fd[0];
            channels_group[i][group_num(i, MIMPI_Father)][1] = fd[1];
        }
        if (group_num(i, MIMPI_Left) < n) {
            ASSERT_SYS_OK(channel(fd));
            channels_group[i][group_num(i, MIMPI_Left)][0] = fd[0];
            channels_group[i][group_num(i, MIMPI_Left)][1] = fd[1];
        }
        if (group_num(i, MIMPI_Right) < n) {
            ASSERT_SYS_OK(channel(fd));
            channels_group[i][group_num(i, MIMPI_Right)][0] = fd[0];
            channels_group[i][group_num(i, MIMPI_Right)][1] = fd[1];
        }
    }
    for (int i = 0; i < n; ++i) {
        for (int j = i + 1; j < n; ++j) {
            ASSERT_SYS_OK(channel(fd));
            channels_point_point[i][j][0] = fd[0];
            channels_point_point[i][j][1] = fd[1];
            ASSERT_SYS_OK(channel(fd));
            channels_point_point[j][i][0] = fd[0];
            channels_point_point[j][i][1] = fd[1];
        }
        ASSERT_SYS_OK(pid = fork());
        if (!pid) {
            char rank[20];
            sprintf(rank, "%d", i);
            for (int j = 0; j < 3; ++j) {
                if (group_num(i, j) >= 0 && group_num(i, j) < n) {
                    ASSERT_SYS_OK(dup2(channels_group[i][group_num(i, j)][0], determine_gread(j)));
                    ASSERT_SYS_OK(dup2(channels_group[group_num(i, j)][i][1], determine_gwrite(j)));
                }
            }

            for (int j = 0; j < n; ++j) {
                if (i != j) {
                    //printf("test\n");
                    //printf("assigned read from %d to %d to %d in process %d\n", j, i, determine_read(i, j), i);
                    ASSERT_SYS_OK(dup2(channels_point_point[i][j][0], determine_read(i, j)));
                    //printf("assigned write from %d to %d to %d in process %d\n", i, j, determine_write(i, j), i);
                    ASSERT_SYS_OK(dup2(channels_point_point[j][i][1], determine_write(i, j)));
                }
            }
            for (int j = i; j < n; ++j) {
                for (int k = 0; k <= i; ++k) {
                    if (j != k) {
                        ASSERT_SYS_OK(close(channels_point_point[j][k][0]));
                        ASSERT_SYS_OK(close(channels_point_point[j][k][1]));
                        ASSERT_SYS_OK(close(channels_point_point[k][j][0]));
                        ASSERT_SYS_OK(close(channels_point_point[k][j][1]));
                    }
                }
            }
            for (int j = 0; j < n; ++j) {
                if (group_num(j, MIMPI_Father) >= 0) {
                    ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Father)][0]));
                    ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Father)][1]));                }
                if (group_num(j, MIMPI_Left) < n) {
                    ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Left)][0]));
                    ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Left)][1]));
                }
                if (group_num(j, MIMPI_Right) < n) {
                    ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Right)][0]));
                    ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Right)][1]));
                }
            }
            ASSERT_SYS_OK(setenv("MIMPI_RANK", rank, 0));
            ASSERT_SYS_OK(execvp(prog, args));
        }
        for (int j = 0; j < i; ++j) {
            ASSERT_SYS_OK(close(channels_point_point[i][j][0]));
            ASSERT_SYS_OK(close(channels_point_point[i][j][1]));
            ASSERT_SYS_OK(close(channels_point_point[j][i][0]));
            ASSERT_SYS_OK(close(channels_point_point[j][i][1]));
        }
    }
    for (int j = 0; j < n; ++j) {
        if (group_num(j, MIMPI_Father) >= 0) {
            ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Father)][0]));
            ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Father)][1]));                }
        if (group_num(j, MIMPI_Left) < n) {
            ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Left)][0]));
            ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Left)][1]));
        }
        if (group_num(j, MIMPI_Right) < n) {
            ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Right)][0]));
            ASSERT_SYS_OK(close(channels_group[j][group_num(j, MIMPI_Right)][1]));
        }
    }

    for (int i = 0; i < n; ++i) {
        wait(NULL);
    }

    return 0;
}