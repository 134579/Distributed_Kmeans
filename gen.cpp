#include <random>
#include <ctime>
#include <fstream>
using namespace std;

int N=10000;
int main(int args,char *argv[])
{
	ofstream o1("point0");
	ofstream o2("point1");

	ofstream o3("point2");

	ofstream o4("point3");

	srand(time(NULL));
	int x1=rand()%(N/2);
	int y1=rand()%(N/2);

	int x2=rand()%(N/2)+N/2;
	int y2=rand()%(N/2);


	int x3=rand()%(N/2);
	int y3=rand()%(N/2)+N/2;

	int x4=rand()%(N/2)+N/2;
	int y4=rand()%(N/2)+N/2;
	random_device rd;
	mt19937 gen(rd());
	normal_distribution<> d(0,1000);

	for(int i=0;i<10000;i++)
	{
		o1<<x1+d(gen)<<" "<<y1+d(gen)<<endl;
		o2<<x2+d(gen)<<" "<<y2+d(gen)<<endl;
		o3<<x3+d(gen)<<" "<<y3+d(gen)<<endl;
		o4<<x4+d(gen)<<" "<<y4+d(gen)<<endl;
	}
	o1.close();
	o2.close();
	o3.close();
	o4.close();
	return 0;
}

