#include "csvParser.h"
int main() {
  csvParser parser;
  std::cout << "Please input file name: ";
  std::string filename;
  std::getline(std::cin, filename);
  parser.readFile(filename);
  bool run = true;
  while(run){
    std::cout << "Please choose an operation:" << std::endl;
    std::cout << "1: arithmetic operations on two columns" << std::endl;
    std::cout << "2: column statistics" << std::endl;
    std::cout << "3: join columns" << std::endl;
    std::cout << "4: exit" << std::endl;
    int option;
    std::cin >> option;
    if(option == 4) break;
    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    std::cout << "Please specify the operator and column names: ";
    std::string command;
    std::getline(std::cin, command);
    std::regex opRe("\\s+");
    std::sregex_token_iterator it(command.begin(), command.end(), opRe, -1); 
    std::sregex_token_iterator col1 = it;
    std::sregex_token_iterator oprand = ++it;
    std::sregex_token_iterator col2 = ++it;
    switch(option){
      case 1:
        Arith arithmetic;
        std::cout<< col1->str() << oprand->str() << col2->str() << std::endl;
        if(oprand->str() == "+"){
          arithmetic = ADD;
        }
        else if(oprand->str() == "-"){
          arithmetic = SUB;
        }
        else if(oprand->str() == "*"){
          arithmetic = MULT;
        }
        else{
          arithmetic = DIV;
        }
        parser.columnArithmeticDriver(col1->str(), col2->str(), arithmetic, col1->str()+oprand->str()+col2->str());
        break;
      case 2:
        Stat statistics;
        if(col1->str() == "max"){
          statistics = MAX;
        }
        else if(col1->str() == "min"){
          statistics = MIN;
        }
        else if(col1->str() == "avg"){
          statistics = AVG;
        }
        else if(col1->str() == "med"){
          statistics = MED;
        }
        parser.columnStatDriver(oprand->str(), statistics, col1->str()+oprand->str());
        break;
      case 3:
        std::string file2;
        csvParser parser2;
        std::cout << "Please specify the name of another file: ";
        std::getline(std::cin, file2);
        parser2.readFile(file2);
        Join join;
        if(oprand->str() == "inner"){
          join = INNER;
        }
        else if(oprand->str() == "outer"){
          join = OUTER;
        }
        parser.columnJoin(col1->str(), col2->str(), join, parser2, col1->str()+oprand->str()+col2->str());
        break;
    }
  }
  return 0;
}
