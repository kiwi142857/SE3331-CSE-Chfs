#pragma once

#define RAFT_FILE_OP_ERROR_ON
#ifdef RAFT_FILE_OP_ERROR_ON
#define RAFT_FILE_OP_ERROR(fmt, args...)                                                                               \
    do {                                                                                                               \
        auto now =                                                                                                     \
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) \
                .count();                                                                                              \
        char buf[512];                                                                                                 \
        sprintf(buf, "[%ld][%s:%d] [FILE OP]" fmt "\n", now, __FILE__, __LINE__, ##args);                              \
        std::cerr << buf;                                                                                              \
    } while (0)
#else
#define RAFT_FILE_OP_ERROR(fmt, args...)
#endif