
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"
#include "Aggregate.h"
#include "BPlusSelection.h"
#include "RegularSelection.h"
#include "ScanJoin.h"
#include "SortMergeJoin.h"
#include "MyDB_Table.h"

string createSelectionPredicate(vector <ExprTreePtr> selectionPredicates) {
    string selectionPredicate = "";
    if (selectionPredicates.size() >= 1) {
        selectionPredicate = selectionPredicates[0]->toString();
    }

    for (int i = 1; i < selectionPredicates.size(); i++) {
        string pred = selectionPredicates[i]->toString();
        selectionPredicate = "&& (" + selectionPredicate + ", " + pred + ")";
    }
    cout << "selectionPredicate: " << selectionPredicate << endl;

    return selectionPredicate;
}

MyDB_TableReaderWriterPtr LogicalTableScan :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	cout << "In logical Table Scan execute" << endl;
    // Call this function then call run()
		// RegularSelection (MyDB_TableReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
	// 	string selectionPredicate, vector <string> projections);

    cout << "Input spec table name: " << inputSpec->getName() << endl;
    MyDB_TableReaderWriterPtr input = allTableReaderWriters[inputSpec->getName()];

    MyDB_TableReaderWriterPtr output = make_shared<MyDB_TableReaderWriter>(outputSpec, input->getBufferMgr());

    // string selectionPredicate = createSelectionPredicate(selectionPred);
    string selectionPredicate = "";
    if (selectionPred.size() >= 1) {
        selectionPredicate = selectionPred[0]->toString();
    }

    for (int i = 1; i < selectionPred.size(); i++) {
        string pred = selectionPred[i]->toString();
        selectionPredicate = "&& (" + selectionPredicate + ", " + pred + ")";
    }

    cout << "selectionPredicate: " << selectionPredicate << endl;

    vector <string> projections;
    for (pair <string, MyDB_AttTypePtr> &a : outputSpec->getSchema()->getAtts()) {
        projections.push_back("[" + a.first + "]");
    }

    cout << "running regular selection " << endl;
    RegularSelection myOp (input, output, selectionPredicate, projections);
    myOp.run();

    return output;
}

MyDB_TableReaderWriterPtr LogicalJoin :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	cout << "In logical join execute" << endl;
    // Need to create this function then run it, not sure how to determine if to call sortMergeJoin or ScanJoin
    // SortMergeJoin (MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
	// 	MyDB_TableReaderWriterPtr output, string finalSelectionPredicate, 
	// 	vector <string> projections,
	// 	pair <string, string> equalityCheck, string leftSelectionPredicate,
	// 	string rightSelectionPredicate);

    // Or call this function
    // ScanJoin (MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
    // MyDB_TableReaderWriterPtr output, string finalSelectionPredicate, 
    // vector <string> projections,
    // vector <pair <string, string>> equalityChecks, string leftSelectionPredicate,
    // string rightSelectionPredicate);
	
    MyDB_TableReaderWriterPtr leftInput = leftInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
    MyDB_TableReaderWriterPtr rightInput = rightInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
    MyDB_TableReaderWriterPtr output = make_shared<MyDB_TableReaderWriter>(outputSpec, leftInput->getBufferMgr());
    string finalSelectionPredicate = createSelectionPredicate(outputSelectionPredicate);

    // Not sure how to get projections
    vector <string> projections;

    // Not sure how to get equality checks or if it should be a vector or just one
    vector <pair <string, string>> equalityChecks;

    // Not sure how to get these
    // string leftSelectionPredicate = createSelectionPredicate(leftInputOp->getOutputSelectionPredicate());
    // string rightSelectionPredciate = createSelectionPredicate(rightInputOp->getOutputSelectionPredicate());

    // SortMergeJoin myOp (leftInput, rightInput, output, finalSelectionPredicate, projections, equalityChecks leftSelectionPredicate, rightSelectionPredicate);
    // myOp.run ();
    // return output;

	return nullptr;
}

#endif
