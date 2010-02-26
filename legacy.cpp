#include <process.hpp>

#include <iostream>
#include <string>

#include <tr1/functional>

using std::cout;
using std::endl;
using std::string;

using std::tr1::bind;


void func(const string &first)
{
  cout << first;
}


class Test : public Process
{
protected:
  void operator () ()
  {
    string hello("hello");
    invoke(bind(func, hello));
    cout << " world" << endl;
  }

public:
  Test() {}
};


int main(int argc, char **argv)
{
  Process::wait(Process::spawn(new Test()));
  return 0;
}
