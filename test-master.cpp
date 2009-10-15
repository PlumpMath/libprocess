#include <test.hpp>

using namespace process::record;

class Master : public RecordProcess
{
private:
  int id;

protected:
  void operator () ()
  {
    do {
      switch (receive()) {
      case REGISTER: {
	std::cout << "Master received REGISTER" << std::endl;

	std::string name;
	unpack<REGISTER>(name);

	std::cout << "Registered slave: " << name << std::endl;

	send(from(), pack<OKAY>(id++));
	break;
      }
      case UNREGISTER: {
	std::cout << "Master received UNREGISTER" << std::endl;

	int slave_id;
	unpack<UNREGISTER>(slave_id);

	std::cout << "Unregistered slave id: " << slave_id << std::endl;

	send(from(), pack<OKAY>(0));
	break;
      }
      default: {
	std::cout << "UNKNOWN MESSAGE RECEIVED " << std::endl;
      }
      }
    } while (true);
  }

public:
  Master() : id(0) {}
};


int main(int argc, char **argv)
{
  PID master = Process::spawn(new Master());
  std::cout << master << std::endl;
  Process::wait(master);
}
