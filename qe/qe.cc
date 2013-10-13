#include "qe.h"
#include<cmath>
#include<malloc.h>
using namespace std;
Filter::Filter(Iterator* input, const Condition &condition)
{
	it = 0;
	total = 0;
	//datas.clear();
	//attrs.clear();
	//length.clear();
	int flag1 = 0;
	int value;
	vector<void *> datass;
	vector<Attribute> attr;
	vector<int> lengths;
	int flag, len, offset;
	input->getAttributes(attr);

	void *data1 = malloc(1000);
	void *data = malloc(1000);
	while (input->getNextTuple(data) != QE_EOF) {
		memcpy((char *) data1, (char *) data, 1000);
		if (condition.bRhsIsAttr == true) {
			flag1 = 1;
			for (int i = 0; i < attr.size(); i++) {
				if (attr[i].name == condition.rhsAttr) {
					if (attr[i].type == 2) {
						//void *comp=malloc(1000);
						memcpy(&len, (char *) data1 + offset, sizeof(int));
						memcpy((char *) condition.rhsValue.data,
								(char*) data1 + offset, len + sizeof(int));
					} else {
						memcpy((char *) condition.rhsValue.data,
								(char*) data1 + offset, sizeof(int));
					}
				} else {
					if (attr[i].type == 2) {
						int len;
						memcpy(&len, (char*) data1 + offset, sizeof(int));
						offset += sizeof(int) + len;
					} else {
						offset += sizeof(int); //sizeof(int)=sizeof(real)
					}
				}

			}
		}
		flag = 1;
		offset = 0;
		for (int i = 0; i < attr.size() && flag != 0; i++) {
			if (attr[i].name == condition.lhsAttr) {
				if (attr[i].type == 2) {

					void *comp = malloc(1000);
					memcpy(&len, (char *) data + offset, sizeof(int));
					memcpy((char *) comp, (char*) data + offset + 4, len);
					value = memcmp((char*) comp,
							(char *) condition.rhsValue.data,
							len + sizeof(int));
					switch (condition.op) {
					case 0:
						if (value == 0)
							flag = 1;
						else
							flag = 0;
						break;
					case 1:
						if (value < 0)
							flag = 1;
						else
							flag = 0;
						break;
					case 2:
						if (value > 0)
							flag = 1;
						else
							flag = 0;
						break;
					case 3:
						if (value <= 0)
							flag = 1;
						else
							flag = 0;
						break;
					case 4:
						if (value >= 0)
							flag = 1;
						else
							flag = 0;
						break;
					case 5:
						if (value != 0)
							flag = 1;
						else
							flag = 0;
						break;
					case 6:
						flag = 1;
						break;
					}
					offset = offset + sizeof(int) + len;
					free(comp);
				} else if (attr[i].type == 0) {
					int v1, v2;
					//void *comp=malloc(1000);
					memcpy(&v1, (char*) data + offset, sizeof(int));
					memcpy(&v2, (char*) condition.rhsValue.data, sizeof(int));

					switch (condition.op) {
					case 0:
						if (v1 == v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 1:
						if (v1 < v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 2:
						if (v1 > v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 3:
						if (v1 <= v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 4:
						if (v1 >= v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 5:
						if (v1 != v2)
							flag = 1;
						break;
					case 6:
						flag = 1;
						break;
					}
					offset = offset + sizeof(int);
				} else {
					float v1, v2;
					//void *comp=malloc(1000);
					memcpy(&v1, (char*) data + offset, sizeof(float));
					memcpy(&v2, (char*) condition.rhsValue.data, sizeof(float));
					//cout << " d" << v1 << v2 << endl;
					switch (condition.op) {
					case 0:
						if (v1 == v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 1:
						if (v1 < v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 2:
						if (v1 > v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 3:
						if (v1 <= v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 4:
						if (v1 >= v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 5:
						if (v1 != v2)
							flag = 1;
						else
							flag = 0;
						break;
					case 6:
						flag = 1;
						break;
					}
					offset = offset + sizeof(float);
				}
			} else {
				if (attr[i].type == 2) {
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					offset += sizeof(int) + len;
				} else {
					offset += sizeof(int); //sizeof(int)=sizeof(real)
				}
			}
		}
		if (flag == 1) {
			void *newv = malloc(offset);
			memcpy((char*) newv, (char*) data, offset);
			lengths.push_back(offset);
			datass.push_back(newv);
			total++;
		}

	}
	attrs = attr;
	datas = datass;
	length = lengths;
	attr.clear();
	datass.clear();
	lengths.clear();
	free(data);
}

Filter::~Filter() {
	//length.clear();
	//attrs.clear();
	for (std::vector<void *>::iterator iter = datas.begin();
			iter != datas.end(); ++iter) {
		free((void *) (*iter));
	}
}
RC Filter::getNextTuple(void *data) {
	if (it == total)
		return QE_EOF;
	//cout << "i is" << it << endl;
	memcpy((char *) data, this->datas[it], length[it]);
	it++;

	return 0;
}
void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;
	//cout << " size:" << attrs.size() << endl;

}
Project::Project(Iterator *input, const vector<string> &attrNames) {
	vector<void *> datass;
	vector<Attribute> attr;
	vector<int> lengths;
	it = 0;
	total = 0;
	int flag1 = 0;
	int offset1 = 0;
	int flag, len, offset;
	offset = 0;

	input->getAttributes(attr);
	void *data1;
	void *data = malloc(1000);
	for (int i = 0; i < attr.size(); i++) {
		flag1 = 0;
		for (int j = 0; j < attrNames.size() && flag1 != 1; j++) {
			//flag1=0;
			if (attr.at(i).name == attrNames.at(j)) {
				attrs.push_back(attr[i]);
				flag1 = 1;
			}
		}
	}

	while (input->getNextTuple(data) != QE_EOF) {
		data1 = malloc(1000);
		offset1 = 0;
		offset = 0;
		for (int i = 0; i < attr.size(); i++) {
			flag = 0;
			for (int j = 0; j < attrNames.size() && flag == 0; j++) {
				if (attr[i].name == attrNames[j]) {
					flag = 1;
				}
			}
			if (flag == 1) {
				if (attr[i].type == 2) {
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					memcpy((char*) data1 + offset1, &len, sizeof(int));
					memcpy((char *) data1 + offset1, (char *) data + offset + 4,
							len);
					offset += sizeof(int) + len;
					offset1 = offset1 + sizeof(int) + len;
				} else {
					memcpy((char *) data1 + offset1, (char *) data + offset, 4);
					offset += sizeof(int); //sizeof(int)=sizeof(real)
					offset1 = offset1 + 4;
				}
			} else {
				if (attr[i].type == 2) {
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					offset += sizeof(int) + len;
				} else {
					offset += sizeof(int); //sizeof(int)=sizeof(real)
				}
			}
		}
		datass.push_back(data1);
		lengths.push_back(offset1);
		total++;

	}
	//attrs=attr;
	datas = datass;
	length = lengths;
	attr.clear();
	datass.clear();
	lengths.clear();
	free(data);
	//free(data1);

}
Project::~Project() {
	attrs.clear();
	length.clear();
	for (std::vector<void *>::iterator iter = datas.begin();
			iter != datas.end(); ++iter) {
		free((void *) (*iter));
	}
}
RC Project::getNextTuple(void *data) {
	if (it == total)
		return QE_EOF;

	memcpy((char *) data, (char *) this->datas[it], this->length[it]);
	it++;

	return 0;
}
void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;

}
NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition,
		const unsigned numPages) {
	it = 0;
	int offset1;
	total = 0;

	attrs.clear();

	void *temp1 = malloc(1000);
	void *temp2 = malloc(1000);
	AttrType t;

	int value;
	vector<void *> datass;

	vector<Attribute> attr1;
	vector<Attribute> attr;
	leftIn->getAttributes(attr);
	rightIn->getAttributes(attr1);
	int length1;
	int length2;
	int flag1 = 0;
	attrs = attr;

	attrs.insert(attrs.end(), attr1.begin(), attr1.end());
	vector<int> lengths;
	int offset2 = 0;
	int flag, len, offset;

	//cout << "attr size :" << attrs.size() << endl;
	void *data1 = malloc(1000);
	void *data2 = malloc(1000);
	void *data = malloc(1000);
	while (leftIn->getNextTuple(data) != QE_EOF) {
		offset = 0;

		flag = 0;
		offset = 0;

		for (int i = 0; i < (int) attr.size(); i++) {

			if (attr[i].name == condition.lhsAttr) {
				t = attrs[i].type;

				if (attrs[i].type == 2) {
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));

					memcpy((char *) temp1, (char *) data + offset, len + 4);
					offset += sizeof(int) + len;
					length1 = len;

				} else {
					memcpy((char *) temp1, (char *) data + offset, 4);
					offset += sizeof(int); //sizeof(int)=sizeof(real)

				}
			} else {
				if (attrs[i].type == 2) {
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					offset += sizeof(int) + len;
				} else {
					offset += sizeof(int);
				}
			}
		}
		rightIn->setIterator();
		while (rightIn->getNextTuple(data1) != QE_EOF) {


			offset2 = 0;
			for (int i = 0; i < (int) attr1.size(); i++) {

				if (attr1[i].name == condition.rhsAttr) {


					if (attr1[i].type == 2) {
						int len;
						memcpy(&len, (char*) data1 + offset2, sizeof(int));

						memcpy((char *) temp2, (char *) data1 + offset2,
								len + 4);

						offset2 = offset2 + sizeof(int) + len;
						length2 = len;
					} else {
						memcpy((char *) temp2, (char *) data1 + offset2, 4);

						offset2 = offset2 + 4;
					}
				} else {
					if (attr1[i].type == 2) {
						int len;
						memcpy(&len, (char*) data1 + offset2, sizeof(int));
						offset2 += sizeof(int) + len;
					} else {
						offset2 += sizeof(int);
					}
				}
			}
			flag = 0;

			if (t == 2) {

				char *value1 = (char*) malloc(length1 + 1);
				char *value2 = (char*) malloc(length2 + 1);

				memcpy((char*) value1, (char*) temp1 + sizeof(int), length1);
				memcpy((char*) value2, (char*) temp2 + sizeof(int), length2);

				value1[length1] = '\0';
				value2[length2] = '\0';

				value = strcmp(value1, value2);
				switch (condition.op) {
				case 0:
					if (value == 0)
						flag = 1;
					break;
				case 1:
					if (value < 0)
						flag = 1;
					break;
				case 2:
					if (value > 0)
						flag = 1;
					break;
				case 3:
					if (value <= 0)
						flag = 1;
					break;
				case 4:
					if (value >= 0)
						flag = 1;
					break;
				case 5:
					if (value != 0)
						flag = 1;
					break;
				case 6:
					flag = 1;
					break;
				}

				free(value1);
				free(value2);
			} else if (t == 0) {
				int v1, v2;

				memcpy(&v1, (char*) temp1, sizeof(int));
				memcpy(&v2, (char*) temp2, sizeof(int));

				switch (condition.op) {
				case 0:
					if (v1 == v2)
						flag = 1;
					break;
				case 1:
					if (v1 < v2)
						flag = 1;

					break;
				case 2:
					if (v1 > v2)
						flag = 1;
					break;
				case 3:
					if (v1 <= v2)
						flag = 1;
					break;
				case 4:
					if (v1 >= v2)
						flag = 1;
					break;
				case 5:
					if (v1 != v2)
						flag = 1;
					break;
				case 6:
					flag = 1;
					break;
				}

			} else {
				float v1, v2;

				memcpy(&v1, (char*) temp1, sizeof(float));
				memcpy(&v2, (char*) temp2, sizeof(float));

				switch (condition.op) {
				case 0:
					if (v1 == v2)
						flag = 1;
					break;
				case 1:
					if (v1 < v2)
						flag = 1;

					break;
				case 2:
					if (v1 > v2)
						flag = 1;
					break;
				case 3:
					if (v1 <= v2)
						flag = 1;
					break;
				case 4:
					if (v1 >= v2)
						flag = 1;
					break;
				case 5:
					if (v1 != v2)
						flag = 1;
					break;
				case 6:
					flag = 1;
					break;
				}

			}
			if (flag == 1) {
				void *newv = malloc(offset + offset2);
				memcpy((char*) newv, (char*) data, offset);
				memcpy((char*) newv + offset, (char*) data1, offset2);
				lengths.push_back(offset + offset2);
				datass.push_back(newv);
				total++;
			}

		}

	}
	datas = datass;
	length = lengths;
	free(data);
	free(data1);
	free(data2);
	free(temp1);
	free(temp2);
}


NLJoin::~NLJoin() {

	for (std::vector<void *>::iterator iter = datas.begin();
			iter != datas.end(); ++iter) {
		free((void *) (*iter));
	}
}
RC NLJoin::getNextTuple(void *data) {
	if (it == total)
		return QE_EOF;

	memcpy((char *) data, this->datas[it], this->length[it]);
	it++;

	return 0;
}
void NLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;

	unsigned i;


}
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn,
		const Condition &condition, const unsigned numPages) {
	it = 0;
	int offset1;
	total = 0;

	void *temp1 = malloc(1000);
	void *temp2 = malloc(1000);
	AttrType t;

	int value;
	vector<void *> datass;

	vector<Attribute> attr1;
	vector<Attribute> attr;
	leftIn->getAttributes(attr);
	rightIn->getAttributes(attr1);
	int length1;
	int length2;
	int flag1 = 0;
	attrs = attr;

	attrs.insert(attrs.end(), attr1.begin(), attr1.end());
	vector<int> lengths;
	int offset2 = 0;
	int flag, len, offset;

	//cout << "attr size :" << attrs.size() << endl;
	void *data1 = malloc(1000);

	void *data = malloc(1000);
	while (leftIn->getNextTuple(data) != QE_EOF) {
		offset = 0;

		flag = 0;
		offset = 0;

		for (int i = 0; i < (int) attr.size(); i++) {

			if (attr[i].name == condition.lhsAttr) {
				t = attrs[i].type;

				if (attrs[i].type == 2) {
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));

					memcpy((char *) temp1, (char *) data + offset, len + 4);
					offset += sizeof(int) + len;
					length1 = len;

				} else {
					memcpy((char *) temp1, (char *) data + offset, 4);
					offset += sizeof(int);

				}
			} else {
				if (attrs[i].type == 2) {
					int len;
					memcpy(&len, (char*) data + offset, sizeof(int));
					offset += sizeof(int) + len;
				} else {
					offset += sizeof(int);
				}
			}
		}

		switch (condition.op) {
		case EQ_OP:
			rightIn->setIterator(temp1, temp1, true, false);
			break;
		case 1:
			rightIn->setIterator(NULL, NULL, true, true);
			break;
		case 2:
			rightIn->setIterator(NULL, NULL, true, true);
			break;
		case 3:
			rightIn->setIterator(NULL, temp1, true, false);
			break;
		case 4:
			rightIn->setIterator(temp1, NULL, false, true);
			break;
		case 5:
			rightIn->setIterator(NULL, temp1, true, true);
			break;
		case 6:
			rightIn->setIterator(temp1, NULL, true, true);
			break;
		}
		while (rightIn->getNextTuple(data1) != QE_EOF) {


			offset2 = 0;
			for (int i = 0; i < (int) attr1.size(); i++) {
				if (attr1[i].type == 2) {
					int len;
					memcpy(&len, (char*) data1 + offset2, sizeof(int));

					offset2 = offset2 + sizeof(int) + len;

				} else {

					offset2 = offset2 + 4;
				}
			}


			offset2 = 0;
			for (int i = 0; i < (int) attr1.size(); i++) {

				if (attr1[i].name == condition.rhsAttr) {

					//offset=0;
					if (attr1[i].type == 2) {
						int len;
						memcpy(&len, (char*) data1 + offset2, sizeof(int));

						memcpy((char *) temp2, (char *) data1 + offset2,
								len + 4);

						offset2 = offset2 + sizeof(int) + len;
						length2 = len;
					} else {
						memcpy((char *) temp2, (char *) data1 + offset2, 4);

						offset2 = offset2 + 4;
					}
				} else {
					if (attr1[i].type == 2) {
						int len;
						memcpy(&len, (char*) data1 + offset2, sizeof(int));
						offset2 += sizeof(int) + len;
					} else {
						offset2 += sizeof(int);
					}
				}
			}
			flag = 0;

			if (t == 2) {

				char *t1 = (char*) malloc(length1 + 1);
				char *t2 = (char*) malloc(length2 + 1);

				memcpy((char*) t1, (char*) temp1 + sizeof(int), length1);
				memcpy((char*) t2, (char*) temp2 + sizeof(int), length2);

				t1[length1] = '\0';
				t2[length2] = '\0';

				value = strcmp(t1, t2);
				switch (condition.op) {
				case 0:
					if (value == 0)
						flag = 1;
					break;
				case 1:
					if (value < 0)
						flag = 1;
					break;
				case 2:
					if (value > 0)
						flag = 1;
					break;
				case 3:
					if (value <= 0)
						flag = 1;
					break;
				case 4:
					if (value >= 0)
						flag = 1;
					break;
				case 5:
					if (value != 0)
						flag = 1;
					break;
				case 6:
					flag = 1;
					break;
				}
				//offset = offset + sizeof(int) + len;
				free(t1);
				free(t2);
			} else if (t == 0) {
				int v1, v2;

				memcpy(&v1, (char*) temp1, sizeof(int));
				memcpy(&v2, (char*) temp2, sizeof(int));

				switch (condition.op) {
				case 0:
					if (v1 == v2)
						flag = 1;
					break;
				case 1:
					if (v1 < v2)
						flag = 1;

					break;
				case 2:
					if (v1 > v2)
						flag = 1;
					break;
				case 3:
					if (v1 <= v2)
						flag = 1;
					break;
				case 4:
					if (v1 >= v2)
						flag = 1;
					break;
				case 5:
					if (v1 != v2)
						flag = 1;
					break;
				case 6:
					flag = 1;
					break;
				}
				//offset = offset + sizeof(int);
			} else {
				float v1, v2;
				//void *comp=malloc(1000);
				memcpy(&v1, (char*) temp1, sizeof(float));
				memcpy(&v2, (char*) temp2, sizeof(float));

				switch (condition.op) {
				case 0:
					if (v1 == v2)
						flag = 1;
					break;
				case 1:
					if (v1 < v2)
						flag = 1;

					break;
				case 2:
					if (v1 > v2)
						flag = 1;
					break;
				case 3:
					if (v1 <= v2)
						flag = 1;
					break;
				case 4:
					if (v1 >= v2)
						flag = 1;
					break;
				case 5:
					if (v1 != v2)
						flag = 1;
					break;
				case 6:
					flag = 1;
					break;
				}
				//offset = offset + sizeof(float);
			}

			//
			if (condition.op == NE_OP) {
				if (flag != 1) {
					void *newv = malloc(offset + offset2);
					memcpy((char*) newv, (char*) data, offset);
					memcpy((char*) newv + offset, (char*) data1, offset2);
					lengths.push_back(offset + offset2);
					datass.push_back(newv);
					total++;
				}
			} else {
				void *newv = malloc(offset + offset2);
				memcpy((char*) newv, (char*) data, offset);
				memcpy((char*) newv + offset, (char*) data1, offset2);
				lengths.push_back(offset + offset2);
				datass.push_back(newv);
				total++;
				//free(newv);

			}
		}

	}
	datas = datass;
	length = lengths;
	free(data);
	free(data1);
	//free(data2);
	free(temp1);
	//free(temp2);
}

// ... the rest of your implementations go here
INLJoin::~INLJoin() {
	//length.clear();
	//attrs.clear();
	for (std::vector<void *>::iterator iter = datas.begin();
			iter != datas.end(); ++iter) {
		free((void *) (*iter));
	}
}
RC INLJoin::getNextTuple(void *data) {
	if (it == total)
		return QE_EOF;

	memcpy((char *) data, this->datas[it], this->length[it]);
	it++;

	return 0;
}
void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->attrs;

	unsigned i;

	// For attribute in vector<Attribute>, name it as rel.attr

}
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
	//cout << "in" << endl;
//unsigned i;
	it = 0;
	total = 0;
	int temp2;

	int offset;

	vector<Attribute> attr;

	input->getAttributes(attr);

	void *data = malloc(1000);

	float fmin = 0.0, fmax = 0.0, fsum = 0.0, avg = 0.0, temp1;
	int temp, imin = 0, imax = 0, isum = 0;
	float count = 0.0;

	while (input->getNextTuple(data) != QE_EOF) {

		offset = 0;
		for (int i = 0; i < attr.size(); i++) {

			if (attr[i].name == aggAttr.name) {
				if (aggAttr.type == TypeInt) {

					memcpy(&temp, (char*) data + offset, sizeof(int));
					if (i == 0) {
						imin = temp;
					}
					if (temp < imin) {
						imin = temp;

					}
					if (temp > imax) {
						imax = temp;
					}
					isum = isum + temp;
					//ftmp = (float)(itmp);
				} else if (aggAttr.type == TypeReal) {
					memcpy(&temp1, (char*) data + offset, sizeof(int));
					if (i == 0) {
						fmin = temp;
					}
					if (temp1 < fmin) {
						fmin = temp1;

					}
					if (temp1 > fmax) {
						fmax = temp1;
					}
					fsum = fsum + temp1;
				}
				count++;

				offset += sizeof(int);
			} else {
				if (attr[i].type == TypeVarChar) {
					memcpy(&temp2, (char*) data + offset, sizeof(int));
					offset += sizeof(int) + temp2;
				} else
					offset += sizeof(int);
			}
		}
	}
	void *data1 = malloc(4);
	string name;
	Attribute correct;

	switch (aggAttr.type) {
	case TypeReal:
		avg = fsum / count;
		switch (op) {
		case MIN:
			memcpy((char*) data1, &fmin, sizeof(float));
			name = "MIN";
			correct.type = TypeReal;
			break;
		case MAX:
			memcpy((char*) data1, &fmax, sizeof(float));
			name = "MAX";
			correct.type = TypeReal;
			break;
		case SUM:
			memcpy((char*) data1, &fsum, sizeof(float));
			name = "SUM";
			correct.type = TypeReal;
			break;
		case AVG:
			memcpy((char*) data1, &avg, sizeof(float));
			name = "AVG";
			correct.type = TypeReal;
			break;
		case COUNT:
			memcpy((char*) data1, &count, sizeof(float));
			name = "COUNT";
			correct.type = TypeReal;
			break;
		}
		break;
	case TypeInt:
		avg = ((float) isum) / count;
		fmin = (float) imin;
		fmax = (float) imax;
		fsum = (float) isum;
		switch (op) {
		case MIN:
			memcpy((char*) data1, &fmin, sizeof(int));
			name = "MIN";
			correct.type = TypeReal;
			break;
		case MAX:
			memcpy((char*) data1, &fmax, sizeof(int));
			name = "MAX";
			correct.type = TypeReal;
			break;
		case SUM:
			memcpy((char*) data1, &fsum, sizeof(int));
			name = "SUM";
			correct.type = TypeReal;
			break;
		case AVG:
			memcpy((char*) data1, &avg, sizeof(float));
			name = "AVG";
			correct.type = TypeReal;
			break;
		case COUNT:
			memcpy((char*) data1, &count, sizeof(int));
			name = "COUNT";
			correct.type = TypeReal;
			break;
		}
		break;
	}

	datas.push_back(data1);
	length.push_back(4);
	total++;
	name += "(" + aggAttr.name + ")";
	correct.name = name;
	correct.length = 4;
	attrs.push_back(correct);

	free(data);
//free(data1);
}

Aggregate::~Aggregate() {
	for (std::vector<void *>::iterator iter = datas.begin();
			iter != datas.end(); ++iter) {
		free((void *) (*iter));
	}
}

RC Aggregate::getNextTuple(void *data) {
	if (it == total)
		return QE_EOF;

	memcpy((char*) data, (char*) this->datas[it], this->length[it]);
	it++;
	return 0;
}
void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();

	attrs = this->attrs;
}
