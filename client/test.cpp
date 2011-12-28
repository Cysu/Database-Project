#include <kchashdb.h>

using namespace std;
using namespace kyotocabinet;

int main() {
	HashDB db;
	if (!db.open("casket.kch", HashDB::OWRITER | HashDB::OCREATE)) {
		cerr << "open error: " << db.error().name() << endl;
	}

	if (!db.set("foo", "hop")) {
		cerr << "set error: " << db.error().name() << endl;
	}

	string value;
	if (db.get("foo", &value)) {
		cout << value << endl;
	} else {
		cerr << "get error: " << db.error().name() << endl;
	}

	DB::Cursor* cur = db.cursor();
	cur->jump();
	string ckey, cvalue;
	while (cur->get(&ckey, &cvalue, true)) {
		cout << ckey << ":" << cvalue << endl;
	}
	delete cur;

	if (!db.close()) {
		cerr << "close error: " << db.error().name() << endl;
	}

	return 0;
}

