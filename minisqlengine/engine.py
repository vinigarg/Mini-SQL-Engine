import re
import sys
import sqlparse
from sqlparse.sql import Where

schema=dict() # of available tables
error =0
errormsg=""
joinPairs=[]

def readMetaData():
	with open("metadata.txt","r") as metafile:
		for line in metafile:
			if line.startswith("<begin_table>"):
				col=[]
				next_line=1
				continue
			if line.startswith("<end_table>"):
				schema[tablename] = col
				continue
			if next_line==1:
				tablename=line[:len(line)-1]
				next_line=0
				continue
			line = line[:len(line)-1]
			col.append(line)
				
def readTable(tablename): # tablename stored in schema
	tablename += ".csv"
	table=[]
	with open(tablename,"r") as tbl:
		for line in tbl:
			line = line.replace('"', '')
			line = line.replace('\'', '')
			line = line.replace(' ', '')
			if line.endswith("\r\n"):
				line = line[:len(line)-2]
			col= line.split(",")
			col = map(int, col) ## convert list of string to int
			table.append(col)
	return table

def joinTable(tables_in_query):
	tablename = tables_in_query.split(",")
	tables=[]
	if len(tablename) == 1:
		return readTable(tablename[0])
	else:
		# join operations
		table=readTable(tablename[0])
		for i in xrange(1,len(tablename)):
			t=readTable(tablename[i])
			temp_table=[]
			for j in xrange(0,len(table)):
				for k in xrange(0,len(t)):
					temp_table.append(table[j]+t[k])
					pass
			table=temp_table
	return table

def generateColumnSpace(tables):
	tables_in_query = tables.split(",")
	columnsSpace =[]
	for x in xrange(0,len(tables_in_query)):
		if tables_in_query[x] in schema:
			table_schema = schema [tables_in_query[x]]
			for y in xrange(0,len(table_schema)):
				columnsSpace.append(tables_in_query[x] + "." + table_schema[y])
		else:
			return []
	
	return columnsSpace	

def distinctFormat(intermediate):
	records = intermediate.split("\n")
	newResult=[]
	for x in xrange(0,len(records)):
		if records[x] not in newResult:
			newResult.append(records[x])
	result = '\n'.join(newResult)
	return result

def evaluateColumnAggregate(columns):
	col = columns.lower().split(",")
	agg=0
	norm=0
	for x in xrange(0,len(col)):
		col[x]=col[x].replace(" ","")
		if col[x].startswith("max") or col[x].startswith("min") or col[x].startswith("sum") or col[x].startswith("avg") :
		   agg+=1
		else :
			norm+=1
	if (agg>0 and norm==0) or (agg==0 and norm>0) :
		return agg
	return -1

def MAX(col , table):
	m = table[0][col]
	for x in xrange(1,len(table)):
		if m< table[x][col]:
			m=table[x][col]
	return m

def MIN(col , table):
	m = table[0][col]
	for x in xrange(1,len(table)):
		if m> table[x][col]:
			m=table[x][col]
	return m

def SUM(col , table):
	m = table[0][col]
	for x in xrange(1,len(table)):
		m+=table[x][col]
	return m

def AVERAGE(col , table):
	return float(SUM(col,table) / len(table))


def evalColumns(columns, table , tables_in_query):
	result=""
	col = columns.lower().split(",")
	for x in xrange(0,len(col)):
		col[x] = col[x].replace(" ","")
		if col[x].startswith("max("):
			index=selectColumn(col[x][4:-1].upper(),tables_in_query)
			temp = str(MAX(index[0] , table))
			result += temp+","

		elif col[x].startswith("min("):
			index=selectColumn(col[x][4:-1].upper(),tables_in_query)
			temp = str(MIN(index[0] , table))
			result += temp+","

		elif col[x].startswith("sum("):
			index=selectColumn(col[x][4:-1].upper(),tables_in_query)
			temp = str(SUM(index[0] , table))
			result += temp+","

		elif col[x].startswith("avg(") :
			index=selectColumn(col[x][4:-1].upper(),tables_in_query)
			temp = str(AVERAGE(index[0] , table))
			result += temp+","

	result=result[:-1]
	return result

def generateColumnHeader (columns , tables_in_query):
	columnsSpace = generateColumnSpace(tables_in_query)
	col=[]
	for x in xrange(0,len(columns)):
		col.append(columnsSpace[columns[x]])		
	return ','.join(col)

def locate(column, columnsSpace):
	result=-1
	count=0
	for x in xrange(0,len(columnsSpace)):
		a="."+column
		if columnsSpace[x].endswith(a) or columnsSpace[x].upper()==column:
			result= x
			count+=1
	if count!=1:
		return -1
	return result

def getOperands(condition):
	i=0
	parts=[]
	while i < len(condition):
		if condition[i]=='<' and condition[i+1]!='=':
			op="<"
			i+=1
		elif condition[i]=='<' and condition[i+1]=='=':
			op="<="
			i+=1
		elif condition[i]=='>' and condition[i+1]!='=':
			op=">"
			i+=1
		elif condition[i]=='>' and condition[i+1]=='=':
			op=">="
			i+=1
		elif condition[i]=='=' and (condition[i+1]!='=' or condition[i-1]!='=' or condition[i+1]!='<' or 
									condition[i+1]!='>' or condition[i+1]!='!'):
			op="="
			i+=1
		elif condition[i]=='!' and condition[i+1]=='=':
			op="!="
			i+=1
		i+=1
	parts = condition.split(op)
	if op!="=":
		parts.append(op)
	else:
		parts.append("==")
	return parts

def handleWhereCondition(table , condition, columnsSpace):
	global errormsg,error
	try:
		list_of_connectors = []
		temp=condition.upper().split(" ")
		for x in xrange(0,len(temp)):
			if temp[x]=="AND" or temp[x]=="OR":
				list_of_connectors.append(temp[x])

		delimiters="AND","OR"
		regexPattern = '|'.join(map(re.escape, delimiters))
		words = re.split(regexPattern, condition.upper())

		x=0
		result=[]

		while x < len(words) and error==0:
			words[x]=words[x].replace(" ","")
			parts = getOperands(words[x])
			left=locate(parts[0],columnsSpace)
			right=locate(parts[1],columnsSpace)
			if left >-1 and right>-1:
				parts[0]=parts[0].replace(parts[0],"table[x]["+str(left)+"]")
				parts[1]=parts[1].replace(parts[1],"table[x]["+str(right)+"]")

			elif left>-1 :
				parts[0]=parts[0].replace(parts[0],"table[x]["+str(left)+"]")

			else :
				error=1
				errormsg="Syntax Error : Where Clause"
		 	
		 	l=parts[0],parts[1]
		 	words[x]= parts[2].join(l)
		 	x+=1

		new_condition=words[0]+" "
		i=0
		for x in xrange(1,len(words)):
			new_condition+=list_of_connectors[i].lower()+" "
			new_condition+=words[x]+" "
			i+=1
		
		for x in xrange(0,len(table)):
			if eval(new_condition):
				result.append(table[x])

		return result
	except Exception :
		error=1
		errormsg="Syntax Error : Where Clause"
		return []

def naturalJoinColumnSelection(condition, columnsSpace):
	global joinPairs,errormsg,error
	try:
		delimiters="AND","OR"
		regexPattern = '|'.join(map(re.escape, delimiters))
		words = re.split(regexPattern, condition.upper())
		result=[]
		x=0
		while x < len(words) and error==0:
			words[x]=words[x].replace(" ","")
			parts = getOperands(words[x])
			if "." in parts[0] and "." in parts[1]:
				dotL = parts[0].index(".")
				dotR = parts[1].index(".")
				if parts[0][dotL:]==parts[1][dotR:] and parts[2]=="==":
					pair = locate(parts[0],columnsSpace), locate(parts[1],columnsSpace)
					joinPairs.append(pair)
			x+=1
	except Exception:
		errormsg="Syntax Error : attributes in WHERE clause"
		error=1


def selectColumn (col , tables):
	columnsSpace = generateColumnSpace(tables)
	if len(columnsSpace) ==0:
		return []

	columns=[]
	if col=="*":
		columns=columnsSpace
	else:
		col = col.replace(" ","")
		columns = col.split(",")

	result_columns =[]
	for i in xrange(0,len(columns)):
		try:
			result_columns.append(columnsSpace.index(columns[i]))
		except ValueError:
			index=-1
			occ =0
			for j in xrange(0,len(columnsSpace)):
				if columnsSpace[j].endswith("."+columns[i]):
					index =j
					occ+=1
			if occ==1:
				result_columns.append(index)
			else:
				return []
	#remove repeataions 
	# keep first column of result_columns which is unique
	if len(joinPairs)>0:
		for x in xrange(0,len(joinPairs)):
			if joinPairs[x][0] in result_columns and joinPairs[x][1] in result_columns:
				ind1 = result_columns.index(joinPairs[x][0])
				ind2 = result_columns.index(joinPairs[x][1])
				if ind1< ind2:
					del result_columns[ind2]
				else :
					del result_columns[ind1]
				
	return result_columns

########################################################################################################################
readMetaData()
arg = sys.argv[1]
sql = arg.split(";")
# if arg[-1] != ";" :
# 	error=1
# 	errormsg="Semicolon is missing."

for q in xrange(0,len(sql)):
	query = sql[q]
	if len(query)==0:
		continue

	parsed = sqlparse.parse(query)
	stmt = parsed[0]
	number_of_tokens = len(stmt.tokens)
	tokens = map(str , stmt.tokens)

	columns=""
	tables_in_query=""
	distinct=0
	condition = tokens[-1]
	WHERE=0
	x=0
	while x < number_of_tokens-1:
		if tokens[x].upper() == "SELECT":
			if tokens[x+2].upper() == "DISTINCT":
				distinct=1
				x+=2
			columns = tokens[x+2]
			x+=4
		elif tokens[x].upper() == "FROM":
			tables_in_query = tokens[x+2]
			x+=4
		elif tokens[x].upper() == "DISTINCT":
			distinct=1
			x+=1
		elif tokens[x] == " ":
		 	x=x+1
		else:
			error=1
			errormsg = "Syntax error around token : "+tokens[x]
			break

	if condition.upper().startswith("WHERE"):
		condition=condition[6:]
		WHERE=1

	columns = columns.replace(" ","")
	tables_in_query = tables_in_query.replace(" ","")
	agg=evaluateColumnAggregate(columns)
	if agg==-1 :
		error=1
		errormsg ="Syntax error near columns and aggregate function "+columns

	table =[]
	result=""
	header=""

	if error==0:
		try:
			table = joinTable(tables_in_query)
		except IOError :
			if error==0:
				error=1
				errormsg = "Syntax error : table not found : "+ tables_in_query
	
	if error==0:
		if WHERE==1:
			columnsSpace = generateColumnSpace(tables_in_query)
			table = handleWhereCondition(table , condition , columnsSpace)
			if agg!=0:
				header = columns
			naturalJoinColumnSelection(condition,columnsSpace)
	
	if error==0:
		if agg==0:
			result_columns = selectColumn (columns , tables_in_query)
			header = generateColumnHeader(result_columns, tables_in_query)
			if len(result_columns)==0 and error==0:
				error=1
			 	errormsg = "Syntax error : near column projection : "+columns
			else:
				for i in xrange(0,len(table)):
					for j in xrange(0,len(result_columns)):
						result+=str(table[i][result_columns[j]])+","
					result=result[:-1]
					result+="\n"
				result=result[:-1]

		else :
			try :
				header = columns
				if len(header)>0:
					result += evalColumns(columns, table , tables_in_query)
				else :
					result="NULL"
			except IndexError:
				error=1
				errormsg="Syntax Error : Unidentified column in aggregate function"

		if distinct==1:
			result=distinctFormat(result)
		
		if error==0 :
			if result=="":
				print "Empty Set"
			else:
				print header +"\n"+ result
		else :
			print errormsg
	else :
		print errormsg
	error=0
	errormsg=""
