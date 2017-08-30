====================================================================================
INTRODUCTION  
====================================================================================
  This project is part of the assignments for the coursework CS 525 Advance Database Organization, under the
  guidance of Prof. Boris Galvic.
  The code is written by the students, it is not optimized and is not meant for production.
  This project can be taken as a reference, but code should not be copied. At universities copying the code is Plagiarism, which may lead to serious consequences.

====================================================================================
ASSIGNMENTS
====================================================================================
  Each assignment is build on top of module from the previous assignment.
  Following are the list of assignments that were in the coursework.
  Assignment 1 - Storage Manager: Implement a storage manager that allows read/writing of blocks to/from a file on disk
  Assignment 2 - Buffer Manager: Implement a buffer manager that manages a buffer of blocks in memory including reading/flushing to disk and block replacement (flushing blocks to disk to make space for reading new blocks from disk)
  Assignment 3 - Record Manager: Implement a simple record manager that allows navigation through records, and inserting and deleting records
  Assignment 4 - B+-Tree Index: Implement a disk-based B+-tree index structure  

====================================================================================
INTERACTIVE CLIENT
====================================================================================

  Features
  	==> Create table, Insert record, Get record, Update record, Delete record, Scan Table, Delete table
  	==> Previous table name saved for quick access
  	==> update prompts current state of table and provides option to change every column value or skip the change for columns.

  1. Run client.c

  2. There are 7 options displayed.(Not in the same order)
  	CT	—> Create table
  	IR	-> Insert record
  	GR	-> Get record
  	UR	-> Update record
  	DR	-> Delete record
  	ST	-> Scan Table
  	DT	-> Delete table


  3. Some Example Sequential runs are:
  	------------------------------------------------------------------------------------
  	CT
  	Ak
  	2
  	id
  	0
  	name
  	1
  	100
  			Table created
  	------------------------------------------------------------------------------------
  	IR
  	<Enter>		We save previously entered table name for ease or type table name <Ak>
  	11
  	ourdbisbest
  			Record inserted (Note the <record id>: mostly it will be <0.0>)
  	------------------------------------------------------------------------------------
  	GR
  	<Enter>
  	0.0
  			Record displayed
  	------------------------------------------------------------------------------------
  	UR
  	<Enter>
  	0.0
  	22
  	<Enter>		We provide processing of <Enter> - means no change in the value for update
  			Record Updated
  	------------------------------------------------------------------------------------
  	GR
  	<Enter>
  	0.0
  			Record displayed - See changes
  	------------------------------------------------------------------------------------
  	DR
  	<Enter>
  	0.0
  			Record deleted
  	------------------------------------------------------------------------------------
  	GR
  	<Enter>
  	0.0
  			Error displayed
  	------------------------------------------------------------------------------------
  	DT
  	<Enter>
  			Table deleted
  	------------------------------------------------------------------------------------
  	ST
  	In scan Table you can do the following:
  		a > 5
  		a != 5
  		c != true
  		c = false
  		a >= 5 & c != false
  		a <= 6 | a >= 3
  	------------------------------------------------------------------------------------
