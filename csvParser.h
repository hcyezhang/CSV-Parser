#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <vector>
#include <regex>
#include <unordered_map>
#include <unordered_set>

enum Type {INT, FLOAT, STRING};
enum Arith {ADD, SUB, DIV, MULT};
enum Stat {MAX, MIN, AVG, MED};
enum Join {INNER, OUTER};

class Column {
  public:
    Column(Type type_) : type(type_) {}
    virtual ~Column() {}
    Type type;
};

template< typename T >
class TypedColumn : public Column {
  public:
    TypedColumn(Type type_) : Column(type_) {}
    std::vector<T> data;
};

template<class FwdIt, class Compare>
void quicksort(FwdIt first, FwdIt last, Compare cmp = Compare{})
{
    auto const N = std::distance(first, last);
    if (N <= 1) return;
    auto const pivot = *std::next(first, N/2);
    auto const middle1 = std::partition(first, last, [=](auto const& elem){ 
        return cmp(elem, pivot); 
    });
    auto const middle2 = std::partition(middle1, last, [=](auto const& elem){ 
        return !cmp(pivot, elem);
    });
    quicksort(first, middle1, cmp); 
    quicksort(middle2, last, cmp); 
}
class csvParser {
  public:
    typedef std::vector<std::shared_ptr<Column> > DataFrame;
    void readFile(const std::string& fileName);
    template<class T1, class T2> void columnArithmetic(std::shared_ptr<TypedColumn<T1>> col1, std::shared_ptr<TypedColumn<T2>> col2, Arith command, std::string filename);
    template<class T>
    void columnInnerJoin(std::shared_ptr<TypedColumn<T>> col1, std::shared_ptr<TypedColumn<T>> col2, DataFrame & data2, std::string filename);
    template<class T>
    void columnOuterJoin(std::shared_ptr<TypedColumn<T>> col1, std::shared_ptr<TypedColumn<T>> col2, DataFrame & data2, std::string filename);
    void columnArithmeticDriver(std::string col1, std::string col2, Arith command, std::string filename);
    template<class T>
    void columnStat(std::shared_ptr<TypedColumn<T>> col, Stat command, std::string filename);
    void columnStatDriver(std::string col, Stat command, std::string filename);
    void columnJoin(std::string col1, std::string col2, Join command, csvParser&, std::string filename);
    std::string printRow(DataFrame data, int ind);
    DataFrame data;
    std::unordered_map<std::string, int> header;
};

void csvParser::readFile(const std::string& fileName){
  std::ifstream infile;
  infile.open(fileName);
  std::regex commaRe(",");
  std::regex integer("(\\+|-)?[[:digit:]]+");
  std::regex floatNumber("[+-]?\\b[0-9]*\\.?[0-9]+(?:[eE][-+]?[0-9]+)?\\b");
  std::string line;
  std::getline(infile, line);
  std::sregex_token_iterator it(line.begin(), line.end(), commaRe, -1); 
  std::sregex_token_iterator reg_end;
  int counter = 0;

  for(; it != reg_end; it++){
    if(std::regex_match(it->str(), integer)){
      TypedColumn<int>* newColumn = new TypedColumn<int>(INT);
      newColumn->data.push_back(std::stoi(it->str()));
      data.push_back(std::shared_ptr<Column>(newColumn));
      std::cout << "col" << counter << " is of type int" << std::endl;
    }
    else if(std::regex_match(it->str(), floatNumber)){
      TypedColumn<double>* newColumn = new TypedColumn<double>(FLOAT);
      newColumn->data.push_back(std::stod(it->str()));
      data.push_back(std::shared_ptr<Column>(newColumn));
      std::cout << "col" << counter << " is of type double" << std::endl;
    }
    else{
      TypedColumn<std::string>* newColumn = new TypedColumn<std::string>(STRING);
      newColumn->data.push_back(it->str());
      data.push_back(std::shared_ptr<Column>(newColumn));
      std::cout << "col" << counter << " is of type string" << std::endl;
    }
    header["col"+std::to_string(counter)] = counter;
    counter += 1;
  }
  while(std::getline(infile, line)){
    std::sregex_token_iterator it(line.begin(), line.end(), commaRe, -1);
    for(auto vecIt = data.begin(); it != reg_end && vecIt != data.end(); it++, vecIt++){
      switch((*vecIt)->type){
        case INT:
          std::static_pointer_cast<TypedColumn<int> >(*vecIt)->data.push_back(std::stoi(it->str()));
          break;
        case FLOAT:
          std::static_pointer_cast<TypedColumn<double> >(*vecIt)->data.push_back(std::stod(it->str()));
          break;
        case STRING:
          std::static_pointer_cast<TypedColumn<std::string> >(*vecIt)->data.push_back(it->str());
          break;
      }
    }
  }
  infile.close();
  std::cout<< "Successfully finished reading file!" << std::endl;
}

//columnArithmetic computes basic addition, subtraction, multiplication and division given two columns.
template<class T1, class T2>
void csvParser::columnArithmetic(std::shared_ptr<TypedColumn<T1>> col1, std::shared_ptr<TypedColumn<T2>> col2, Arith command, std::string filename){
    std::vector<T1> result;
    result.resize(col1->data.size());
    switch(command){
      case ADD:
        for(int i = 0; i < col1->data.size(); i++){
          result[i] = col1->data[i] + col2->data[i];
        }
        break;
      case MULT:
        for(int i = 0; i < col1->data.size(); i++){
          result[i] = col1->data[i] * col2->data[i];
        }
        break;
      case SUB:
        for(int i = 0; i < col1->data.size(); i++){
          result[i] = col1->data[i] - col2->data[i];
        }
        break;
      case DIV:
        for(int i = 0; i < col1->data.size(); i++){
          result[i] = col1->data[i] / col2->data[i];
        }
        break;
    }
    std::ofstream output(filename);
    for(int i = 0; i < result.size(); i++){
      output << result[i] << std::endl;
    }
    output.close();
}

void csvParser::columnArithmeticDriver(std::string col1, std::string col2, Arith command, std::string filename){
  int ind1 = header[col1];
  int ind2 = header[col2];
  if(data[ind2]->type == FLOAT && data[ind1]->type == INT){
    int tmp = ind2;
    ind2 = ind1;
    ind1 = tmp;
  }
  if(data[ind1]->type == FLOAT && data[ind2]->type == INT){
    columnArithmetic<double, int>(
          std::static_pointer_cast<TypedColumn<double> > (data[ind1]), std::static_pointer_cast<TypedColumn<int>>(data[ind2]),
          command, filename);
  }
  else if(data[ind1]->type == FLOAT && data[ind2]->type == FLOAT){
    columnArithmetic<double, double>(
          std::static_pointer_cast<TypedColumn<double> > (data[ind1]), std::static_pointer_cast<TypedColumn<double>>(data[ind2]),
          command, filename);
  }
  else if(data[ind1]->type == INT && data[ind2]->type == INT){
    columnArithmetic<int, int>(
          std::static_pointer_cast<TypedColumn<int> > (data[ind1]), std::static_pointer_cast<TypedColumn<int>>(data[ind2]),
          command, filename);
  }
}

std::string csvParser::printRow(DataFrame data, int ind){
    std::stringstream buffer;
    for(int i = 0; i < data.size(); i++){
        switch(data[i]->type){
            case FLOAT:
                buffer << std::static_pointer_cast<TypedColumn<double> >(data[i])->data[ind] << ",";
                break;
            case INT:
                buffer << std::static_pointer_cast<TypedColumn<int> >(data[i])->data[ind] << ",";
                break;
            case STRING:
                buffer << std::static_pointer_cast<TypedColumn<std::string> >(data[i])->data[ind] << ",";
                break;
        }
    }
    return buffer.str();
}

template<class T>
void csvParser::columnInnerJoin(std::shared_ptr<TypedColumn<T>> col1, std::shared_ptr<TypedColumn<T>> col2, DataFrame &data2, std::string filename){
  
  std::unordered_map<T,std::vector<int>> map2;//map2 records value of the data(key in the hashmap) and its idx in the col(value in the hasmap); map2 records information for data2
  std::vector<int> intersection;//intersection stores the indices for duplicate records.
  
  for(int i = 0; i < col2->data.size(); i++){
    if(map2.find(col2->data[i]) != map2.end()){
      map2[col2->data[i]].push_back(i);
    }
    else {
      std::vector<int> inds;
      inds.push_back(i);
      map2[col2->data[i]] = inds;
    }
  }
  
  for(int i = 0; i < col1->data.size(); i++){
    if (map2.find(col1->data[i]) != map2.end()){
        intersection.push_back(i); 
    } 
  }
   
  std::ofstream output(filename);
  
  for(int i = 0; i < intersection.size(); i++){
    std::string stri = printRow(data, intersection[i]);
    for(int j = 0; j < map2[col1->data[intersection[i]]].size(); j++){
      std::string str2 = printRow(data2, map2[col1->data[intersection[i]]][j]); 
      output << stri << str2.substr(0,str2.size()-1) << std::endl;    
    }
  }
  
  output.close();
}

//Implementation of Full Outer Join
template<class T>
void csvParser::columnOuterJoin(std::shared_ptr<TypedColumn<T>> col1, std::shared_ptr<TypedColumn<T>> col2, DataFrame &data2, std::string filename){
  std::unordered_map<T,std::vector<int>> map2;
  std::vector<int> intersection;
  std::unordered_set<T> interValue;
  
  for(int i = 0; i < col2->data.size(); i++){
    if(map2.find(col2->data[i]) != map2.end()){
      map2[col2->data[i]].push_back(i);
    }
    else {
      std::vector<int> inds;
      inds.push_back(i);
      map2[col2->data[i]] = inds;
    }
  }
  
  std::vector<int> comp1;//store the complemente of data1 with respect to intersect(data1, data2)
  
  for(int i = 0; i < col1->data.size(); i++){
    if (map2.find(col1->data[i]) != map2.end()){
        intersection.push_back(i); 
        interValue.insert(col1->data[i]);
    }
    else comp1.push_back(i);
  }
   
  std::ofstream output(filename);

  for(int i = 0; i < intersection.size(); i++){ // print inner join of data1 and data2
    std::string stri = printRow(data, intersection[i]);
    for(int j = 0; j < map2[col1->data[intersection[i]]].size(); j++){
      std::string str2 = printRow(data2, map2[col1->data[intersection[i]]][j]); 
      output << stri << str2.substr(0,str2.size()-1) << std::endl;    
    }
  }
  std::string nullstring2 = std::string(data2.size()-1,',');
  for(int i = 0; i < comp1.size(); i++){ // print data1 with non-matchable data2
    std::string str1 = printRow(data, comp1[i]);
    output << str1 << nullstring2 << std::endl;
  }
  std::string nullstring1 = std::string(data.size(),',');
  for(auto it = map2.begin(); it != map2.end(); it++){ // print non-matchable data1 with data2
    if(interValue.find(it->first) == interValue.end()){
       for(int i = 0; i < it->second.size(); i++){
       std::string s = printRow(data2, it->second[i]);  
       output << nullstring1 << s.substr(0, s.size()-1) << std::endl;
       }
    }
  }
  
  output.close();

}
//columnJoin is the driver for columnInnerJoin and columnOuterJoin - cast the input parameters into their corresponding types
void csvParser::columnJoin(std::string col1, std::string col2, Join command, csvParser& parser2, std::string filename){
  int ind1 = header[col1];
  int ind2 = parser2.header[col2];
  switch(command){
    case INNER:
      if(data[ind1]->type == parser2.data[ind2]->type && data[ind1]->type == INT){
        columnInnerJoin<int>(
          std::static_pointer_cast<TypedColumn<int> > (data[ind1]), std::static_pointer_cast<TypedColumn<int>>(parser2.data[ind2]), parser2.data,
          filename);
      }
      else if(data[ind1]->type == data[ind2]->type && data[ind1]->type == FLOAT){
        columnInnerJoin<double>(
          std::static_pointer_cast<TypedColumn<double> >(data[ind1]), std::static_pointer_cast<TypedColumn<double>>(parser2.data[ind2]), parser2.data,
          filename);
      }
      else if(data[ind1]->type == data[ind2]->type){
        columnInnerJoin<std::string>(
          std::static_pointer_cast<TypedColumn<std::string> > (data[ind1]), std::static_pointer_cast<TypedColumn<std::string>>(parser2.data[ind2]), parser2.data,
          filename);
      }
      break;
    case OUTER:
      if(data[ind1]->type == parser2.data[ind2]->type && data[ind1]->type == INT){
        columnOuterJoin<int>(
          std::static_pointer_cast<TypedColumn<int> > (data[ind1]), std::static_pointer_cast<TypedColumn<int>>(parser2.data[ind2]), parser2.data,
          filename);
      }
      else if(data[ind1]->type == data[ind2]->type && data[ind1]->type == FLOAT){
        columnOuterJoin<double>(
          std::static_pointer_cast<TypedColumn<double> >(data[ind1]), std::static_pointer_cast<TypedColumn<double>>(parser2.data[ind2]), parser2.data,
          filename);
      }
      else if(data[ind1]->type == data[ind2]->type){
        columnOuterJoin<std::string>(
          std::static_pointer_cast<TypedColumn<std::string> > (data[ind1]), std::static_pointer_cast<TypedColumn<std::string>>(parser2.data[ind2]), parser2.data,
          filename);
      }
      break;

   }
}

//columStats computes min, max, median and average given a column.
template<class T>
void csvParser::columnStat(std::shared_ptr<TypedColumn<T>> col, Stat command, std::string filename){
    int max = 0;
    int min = 0;
    double res = .0;
    switch(command){
        case MAX:
            for(int i = 0; i < col->data.size(); i++){
                if (col->data[i] > col->data[max]){
                max = i;
                } 
            }
           std::cout << col->data[max] << std::endl;
           break; 
        case MIN:
            for(int i = 0; i < col->data.size(); i++){
                if (col->data[i] < col->data[min]){
                min = i;
                } 
            }
           std::cout << col->data[min] << std::endl;
           break;
        case AVG:
            for(auto e: col->data) res += e;
            res /= col->data.size();
            std::cout << res << std::endl;
            break;
        case MED: // sort the column, then compute median - two cases:1) col.size() is even 2) col.size() is odd.
            std::vector<T> data_copy = col->data;    
            quicksort<typename std::vector<T>::iterator, std::less<T>>(data_copy.begin(),data_copy.end());
            if (col->data.size()%2 == 1){
                std::cout << data_copy[data_copy.size()/2] << std::endl;
            }           
            else{
                std::cout << 0.5* (data_copy[(data_copy.size()-1)/2] + data_copy[(data_copy.size()-1)/2+1]) <<std::endl; 
            }
            break; 
    }
}

void csvParser::columnStatDriver(std::string col, Stat command, std::string filename){
    int ind = header[col];
    if(data[ind]->type == INT){
        columnStat<int>(std::static_pointer_cast<TypedColumn<int>>(data[ind]), command, filename);
    }
    else if (data[ind]->type == FLOAT){
        columnStat<double>(std::static_pointer_cast<TypedColumn<double>>(data[ind]), command, filename);
    }
    else if (data[ind]->type == STRING){
       std::cout << "Calculating statistics on strings is not supported!" << std::endl;
    }
}


