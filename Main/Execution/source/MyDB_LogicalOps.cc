
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
    string selectionPredicate = "bool[true]";

    for (int i = 0; i < selectionPredicates.size(); i++) {
        string pred = selectionPredicates[i]->toString();
        selectionPredicate = "&& (" + selectionPredicate + ", " + pred + ")";
    }
    // cout << "selectionPredicate: " << selectionPredicate << endl;

    return selectionPredicate;
}

MyDB_TableReaderWriterPtr LogicalTableScan :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// cout << "In logical Table Scan execute" << endl;

    // for (auto a : allTableReaderWriters) {
        // cout << "Table name: " << a.first << endl;
    // }

    // cout << "Input spec table name: " << inputSpec->getName() << endl;
    string inputName = inputSpec->getName();
    string sampleAtt = inputSpec->getSchema()->getAtts()[0].first;
    string prefix = sampleAtt.substr(0, sampleAtt.find('_'));
    // cout << "sampleAtt: " << sampleAtt << endl;
    // cout << "prefix: " << prefix << endl;
    MyDB_TableReaderWriterPtr input = make_shared<MyDB_TableReaderWriter>(allTableReaderWriters[inputName]->getTable()->alias(prefix), allTableReaderWriters[inputName]->getBufferMgr());

    MyDB_TableReaderWriterPtr output = make_shared<MyDB_TableReaderWriter>(outputSpec, input->getBufferMgr());
    string selectionPredicate = createSelectionPredicate(selectionPred);

    vector <string> projections;
    for (pair <string, MyDB_AttTypePtr> &a : outputSpec->getSchema()->getAtts()) {
        projections.push_back("[" + a.first + "]");
    }

    // cout << "running regular selection " << endl;
    RegularSelection myOp (input, output, selectionPredicate, projections);
    myOp.run();

    return output;
}

MyDB_TableReaderWriterPtr LogicalJoin :: execute (map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
	map <string, MyDB_BPlusTreeReaderWriterPtr> &allBPlusReaderWriters) {

	// cout << "In logical join execute" << endl;
    // cout << "Outputselection predicate for join: " << endl;
    // for (auto &a: outputSelectionPredicate) {
    //     cout << "    " << a->toString () << "\n";
    // }
	
    MyDB_TableReaderWriterPtr leftInput = leftInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
    MyDB_TableReaderWriterPtr rightInput = rightInputOp->execute(allTableReaderWriters, allBPlusReaderWriters);
    MyDB_TableReaderWriterPtr output = make_shared<MyDB_TableReaderWriter>(outputSpec, leftInput->getBufferMgr());
    string finalSelectionPredicate = createSelectionPredicate(outputSelectionPredicate);

    vector <string> projections;
    for (pair <string, MyDB_AttTypePtr> &a : outputSpec->getSchema()->getAtts()) {
        projections.push_back("[" + a.first + "]");
    }

    // Not sure how to get equality checks or if it should be a vector or just one
    vector <pair <string, string>> equalityChecks;
    // cout << "Critical section" << endl;
    for (auto &a: outputSelectionPredicate) {
        string pred = a->toString ();
        // cout << "pred: " << pred << endl;
        if (pred.rfind("!", 0) == 0) {
            // cout << "pred starts with !, skipping" << endl;
            continue;
        }

        vector<string> results;

        size_t pos = 0;
        while ((pos = pred.find('[', pos)) != std::string::npos) {
            size_t end = pred.find(']', pos);
            if (end == std::string::npos) break;

            results.push_back(pred.substr(pos + 1, end - pos - 1));
            pos = end + 1;
        }
        // cout << "results: " << results.size() << endl;

        // for (const auto& r : results) {
        //     cout << r << endl;
        // }

        if (results.size() != 2) {
            // cout << "CRITICAL ERROR: More than 2 atts in outputSelectionPredicate entry for join: " << pred << endl; 
        } else {
            bool firstInLeft = false;
            bool firstInRight = false;
            bool secondInLeft = false;
            bool secondInRight = false;
            for (auto &b : leftInput->getTable()->getSchema()->getAtts()) {
                if (b.first == results[0]) {
                    firstInLeft = true;
                }
                if (b.first == results[1]) {
                    secondInLeft = true;
                }
                if (firstInLeft && secondInLeft) break;
            }
            for (auto &b : rightInput->getTable()->getSchema()->getAtts()) {
                if (b.first == results[0]) {
                    firstInRight = true;
                }
                if (b.first == results[1]) {
                    secondInRight = true;
                }
                if (firstInRight && secondInRight) break;
            }
            if ((firstInLeft && secondInLeft) || (firstInRight && secondInRight)) {
                continue;
            } else if (firstInLeft && secondInRight) {
                equalityChecks.push_back(make_pair("[" + results[0] + "]", "[" + results[1] + "]"));
            } else if (firstInRight && secondInLeft) {
                equalityChecks.push_back(make_pair("[" + results[1] + "]", "[" + results[0] + "]"));
            }

        }
    }

    // cout << "Created equality checks " << endl;
    // for (auto &a : equalityChecks) {
        // cout << "first: " << a.first << endl;
        // cout << "second: " << a.second << endl;
    // }

    string leftSelectionPredicate = "bool[true]";
    string rightSelectionPredicate = "bool[true]";

    // Chose scan Join or sort merge join based on number of pages
    size_t bufferPages = leftInput->getBufferMgr()->getNumPages();
    int minPages = min(leftInput->getNumPages(), rightInput->getNumPages());

    if (minPages > bufferPages / 2) {
        // cout << "running merge join" << endl;
        SortMergeJoin myOp (leftInput, rightInput, output, finalSelectionPredicate, projections, equalityChecks[0], leftSelectionPredicate, rightSelectionPredicate);
        myOp.run ();
    } else {
        // cout << "running scan join" << endl;
        ScanJoin myOp (leftInput, rightInput, output, finalSelectionPredicate, projections, equalityChecks, leftSelectionPredicate, rightSelectionPredicate);
        myOp.run ();
    }

    return output;
}

#endif
