# coding=utf-8
from xml.etree import ElementTree
from win32com.client import Dispatch
import win32com.client
import sys, os
import re


def ChangeReturnKeyInString(str):
    if str:
        # retStr = str.strip().replace(re.search(r'<.*>', str), '')
        print str
        retStr = re.sub(r'<\w.+>', ' ', str, flags=re.S)
        retStr = re.sub(r'</\w.+>', ' ', retStr, flags=re.S)
        retStr = re.sub(r'<\w.* .+>', ' ', retStr, flags=re.S)
        retStr = retStr.replace('&nbsp;', '')
        retStr = retStr.replace('&quot;', '"')
        return retStr
    else:
        return str


class easy_excel:
    def __init__(self, filename=None):
        self.xlApp = win32com.client.Dispatch('Excel.Application')
        if filename:
            self.filename = filename
            self.xlBook = self.xlApp.Workbooks.Open(self.filename)
        else:
            self.xlBook = self.xlApp.Workbooks.Add()
            self.filename = ''

    def save(self, newfilename=None):
        if newfilename:
            self.filename = newfilename
            self.xlBook.SaveAs(newfilename)
        else:
            self.xlBook.Save()

    def close(self):
        self.xlBook.Close(SaveChanges=0)

    def getCell(self, sheet, row, col):
        sht = self.xlBook.Worksheets(sheet)
        return sht.Cell(row, col).Value

    def setCell(self, sheet, row, col, value):
        sht = self.xlBook.Worksheets(sheet)
        sht.Cells(row, col).Value = value
        sht.Cells(row, col).HorizontalAlignment = 3
        sht.Rows(row).WrapText = True

    def mergeCells(self, sheet, row1, col1, row2, col2):
        sht = self.xlBook.Worksheets(sheet)
        sht.Range(sht.Cells(row1, col1), sht.Cells(row2, col2)).Merge()

    def setBorder(self, sheet, row, col):
        sht = self.xlBook.Worksheets(sheet)
        sht.Cells(row, col).Borders.LineStyle = 1

    def set_col_width(self, sheet):
        sht = self.xlBook.Worksheets(sheet)
        sht.Columns("E:H").ColumnWidth = 30


if __name__ == '__main__':
    if (len(sys.argv) == 1):
        print("Please specified a xml file")
        os.system("pause")
        sys.exit(0)
    else:
        tmpInFile = os.getcwd() + "\\" + sys.argv[1]
        inFile = tmpInFile
        if not tmpInFile.endswith(".xml"):
            outFile = tmpInFile + "-tcexp.xlsx"
            inFile = tmpInFile + ".xml"
        else:
            outFile = tmpInFile[:-4] + "-tcexp.xlsx"

    xls = easy_excel()
    xls.setCell('Sheet1', 1, 1, u"一级目录")
    xls.setCell('Sheet1', 1, 2, u"二级目录")
    xls.setCell('Sheet1', 1, 4, u"用例名称")
    xls.setCell('Sheet1', 1, 3, u"用例编号")
    xls.setCell('Sheet1', 1, 5, u"用例概要说明")
    xls.setCell('Sheet1', 1, 6, u"预置条件")
    xls.setCell('Sheet1', 1, 7, u"操作步骤")
    xls.setCell('Sheet1', 1, 8, u"预期结果")
    xls.set_col_width('Sheet1')

    tree = ElementTree.parse(inFile)
    root = tree.getroot()

    row_flag = 1
    # 一级目录
    for sub1Testsuite in root.findall("testsuite"):
        sub1SuiteId = sub1Testsuite.get("id")
        sub1SuiteName = sub1Testsuite.get("name")
        for sub1TestCase in sub1Testsuite.findall("testcase"):
            row_flag = row_flag + 1
            sub2SuiteName = ""
            title = sub1TestCase.get("name")
            externalid = sub1TestCase.find("externalid").text
            summary = ChangeReturnKeyInString(sub1TestCase.find("summary").text)
            preconditions = ChangeReturnKeyInString(
                sub1TestCase.find("preconditions").text)

            xls.setCell('Sheet1', row_flag, 1, sub1SuiteName)
            xls.setCell('Sheet1', row_flag, 2, sub2SuiteName)
            xls.setCell('Sheet1', row_flag, 3, title)
            xls.setCell('Sheet1', row_flag, 4, externalid)
            xls.setCell('Sheet1', row_flag, 5, summary)
            xls.setCell('Sheet1', row_flag, 6, preconditions)
            stepsNode = sub1TestCase.find("steps")
            actions = ""
            expectedresults = ""
            for stepNode in stepsNode.findall("step"):
                step_number = stepNode.find('step_number').text
                if stepNode.find('actions').text is not None:
                    actions = actions + step_number + stepNode.find('actions').text + '\n'
                else:
                    actions = actions + step_number + '\n'
                if stepNode.find('expectedresults').text is not None:
                    print stepNode.find('expectedresults').text
                    expectedresults = expectedresults + step_number + \
                                        stepNode.find('expectedresults').text + "\n"
                else:
                    expectedresults = expectedresults + step_number + "\n"
            actions = ChangeReturnKeyInString(actions)
            expectedresults = ChangeReturnKeyInString(expectedresults)
            xls.setCell('Sheet1', row_flag, 7, actions)
            xls.setCell('Sheet1', row_flag, 8, expectedresults)

        for sub2Testsuite in sub1Testsuite.findall("testsuite"):
            sub2SuiteId = sub2Testsuite.get("id")
            sub2SuiteName = sub2Testsuite.get("name")
            for sub2TestCase in sub2Testsuite.findall("testcase"):
                row_flag = row_flag + 1
                title = sub2TestCase.get("name")
                externalid = sub2TestCase.find("externalid").text
                summary = ChangeReturnKeyInString(
                    sub2TestCase.find("summary").text)
                preconditions = ChangeReturnKeyInString(
                    sub2TestCase.find("preconditions").text)

                xls.setCell('Sheet1', row_flag, 1, sub1SuiteName)
                xls.setCell('Sheet1', row_flag, 2, sub2SuiteName)
                xls.setCell('Sheet1', row_flag, 4, title)
                xls.setCell('Sheet1', row_flag, 3, externalid)
                xls.setCell('Sheet1', row_flag, 5, summary)
                xls.setCell('Sheet1', row_flag, 6, preconditions)
                actions = ""
                expectedresults = ""
                stepsNode = sub2TestCase.find("steps")
                for stepNode in stepsNode.findall("step"):
                    step_number = stepNode.find('step_number').text
                    if stepNode.find('actions').text is not None:
                        actions = actions + step_number + stepNode.find('actions').text + '\n'
                    else:
                        actions = actions + step_number + '\n'
                    if stepNode.find('expectedresults').text is not None:
                        print stepNode.find('expectedresults').text
                        expectedresults = expectedresults + step_number + \
                                          stepNode.find('expectedresults').text + "\n"
                    else:
                        expectedresults = expectedresults + step_number + "\n"
                actions = ChangeReturnKeyInString(actions)
                expectedresults = ChangeReturnKeyInString(expectedresults)
                xls.setCell('Sheet1', row_flag, 7, actions)
                xls.setCell('Sheet1', row_flag, 8, expectedresults)

    for row in range(2, row_flag):
        for col in range(1, 9):
            xls.setBorder('Sheet1', row, col)
    xls.save(outFile)
    xls.close()

    print("finished.")
    sys.exit(0)