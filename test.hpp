#include <iostream>
#include <string>

#include <record-process.hpp>

namespace process { namespace record {

enum Messages { REGISTER = PROCESS_MSGID, UNREGISTER, OKAY };

RECORD(REGISTER, (std::string /*name*/));
RECORD(UNREGISTER, (int /*id*/));
RECORD(OKAY, (int /*response*/));

}}
