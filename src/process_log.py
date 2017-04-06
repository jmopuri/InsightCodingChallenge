#// your Python code to implement the features could be placed here
#// note that you may use any language, there is no preference towards Python

from datetime import *
import operator

HTTP_FAIL  = 401

class LogEntry:
    # 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
    address = "" # IP address or URL
    strTimeStamp = ""
    DateTimeObj = None
    Resource = ""
    RetunCode = 0 
    data = 0
    
    def ReadEntry(self, strLine):
        try:
            self.address = strLine[: strLine.index("- -")]

            #Read DateTime string and convert it to DateTime object
            self.strTimeStamp = strLine[strLine.index("[")+1 : strLine.index("]")]
            dateSplit1 = self.strTimeStamp.split("-")[0].strip()
            self.DateTimeObj = datetime.strptime(dateSplit1, "%d/%b/%Y:%H:%M:%S")

            #Read the method i.e., port/get stuff
            tempSt = strLine.index('"')+1
            tempEnd = strLine.index('"', tempSt)
            self.Resource = strLine[ tempSt : tempEnd ].strip().split()[1] #1st part is get/post and 2nd part should be resource 

            #Read the return code and data exchanged
            tempStrs = strLine[strLine.rindex('"') + 1 :].strip().split()
            self.RetunCode = tempStrs[0]
            self.data = tempStrs[1]
        except:
            print("Failed parsing :", strLine )
    
    def IsHttpFail(self):
        if self.RetunCode == HTTP_FAIL :
            print("This is HTTP Fail ")
            return True
        else:
            return False
            
    def PrintEntry(self):
        print(self.address, " " , self.strTimeStamp, " ", self.DateTimeObj, " ", self.Resource, " ", self.RetunCode, " ", self.data )
        

class Log:
    LogEntries = {} # this is a disctionary of LogEntry objects
    Resources = {}
    EventTimeStamps = []
    BlockWindows = {} # For feature #4, get block windows while reading the data, 
    Suspects = {}

    def IsThisFailEventInBlockInterval( self, Address, TimeStamp ):
        if not Address in self.BlockWindows:
            return False
        if ( TimeStamp - BlockWindows[Address] ).total_seconds > 300 :
            return True
        else:
            return False
        
    def AddToSuspects( self, Address, TimeStamp ):
        #TODO: ignore if this login fail is with in block window for this address
        
        if not Address in Suspects:
            Suspects[Address] = [] # if this is a new suspect, create a list of fail event
        Suspects[Address].append(TimeStamp) #add login fail event to suspect

        # check if the number of login fails are 3 or more in last 20 secs to add to blocks
        if len(Suspects[Address]) == 3 :
            BlockWindows[Address] = TimeStamp
            del Suspects[Address]

    # Go through all the suspect windows and retire those that are 20 secs are longer
    def RetireSuspects( self, CurrentTime ):
        for key, value in Suspects.items():
            if ((CurrentTime - value[0]).total_seconds >= 20 ): #if it is with 
                #del Suspects(key)
                a =1
            
    
    def ReadLogFile( self, FileName):
        # Check if file exists
        print("File Name: ", FileName )

        # open file for reading
        ResourcePasingFailures = 0
        
        f = open( FileName, "r" )
        try:
            for line in f:
                TempLogEntry = LogEntry()
                TempLogEntry.ReadEntry(line)  # Build a log entry object from the line
                if not TempLogEntry.address in self.LogEntries:
                    self.LogEntries[TempLogEntry.address] = [] #if this new address, create a new list for entries for this address
                self.LogEntries[TempLogEntry.address].append(TempLogEntry)   # Add this entry to this collection

                #build a list of timestamps for the events
                self.EventTimeStamps.append(TempLogEntry.DateTimeObj)
                #self.EventTimeStamps.append(datetime.datetime(100,1,1,11,34,59))
                
                #build a collection of resources with corresponding badwidth use
                try:
                    if TempLogEntry.Resource in self.Resources:
                        self.Resources[TempLogEntry.Resource] = int(self.Resources[TempLogEntry.Resource]) +  int(TempLogEntry.data)
                    else:
                        self.Resources[TempLogEntry.Resource] = int(TempLogEntry.data)
                except:
                    ResourcePasingFailures += 1

                #if( TempLogEntry.IsHttpFail()  
        except:
            print("Failed reading")
        f.close()
        print("Resource Parsing Failures: ", ResourcePasingFailures)
        
    def GetAddressSortedByActivity(self):
        AddressAndActivity = {}
        for key, value in self.LogEntries.items():
            AddressAndActivity[key] = len(value)

        # return a sorted collection by activity in decresing order
        return sorted(AddressAndActivity.items(), key=operator.itemgetter(1), reverse=True)

    def PrintLogDetails( self ):
        for key, value in self.LogEntries.items():
            for item in value:
                item.PrintEntry()
            
        
    def PrintLogSummary( self ):
        print("Length of Log file :", len(self.LogEntries) )
        

NASA_FanLog = Log()
#NASA_FanLog.ReadLogFile(".\\testlog.txt")
#NASA_FanLog.ReadLogFile(".\\log3.txt")
NASA_FanLog.ReadLogFile(".\\log1.txt")
#NASA_FanLog.PrintLogDetails()
NASA_FanLog.PrintLogSummary()

#Feature1: Create a disctionary of addresses with the count of activity
AddressByActivity = NASA_FanLog.GetAddressSortedByActivity()
feaure1 = open(".\\log_output\\hosts.txt", "w")
ctr = 1
for key, value in AddressByActivity:
        feaure1.write(key.strip())
        feaure1.write(",")
        feaure1.write(str(value))
        feaure1.write("\n")
        if( ctr < 10 ):
                ctr += 1
        else:
                break
feaure1.close()

#Feature2: Top 10 resources on the site that consume the most bandwidth
ResourcesSortedByBandwidth = sorted(NASA_FanLog.Resources.items(), key=operator.itemgetter(1), reverse=True)
feature2 = open(".\\log_output\\resources.txt", "w")
ctr = 1
for key, value in ResourcesSortedByBandwidth:
        feature2.write(key)
        feature2.write("\n")
        if( ctr < 10 ):
                ctr += 1
        else:
                break
feature2.close()

#Feature 3: Find the top 10 busiest times
BusyHours = {}
def updateTopTenBusyList(dateTimeStamp, ActivityCount):
    if len(BusyHours) < 10 :
        BusyHours[dateTimeStamp] =  ActivityCount
    else:
        # check if the activity of input item is higher than the last item.
        # If so, add this item to list and delete the last item
        tempCollection = sorted(BusyHours.items(), key=operator.itemgetter(1))
        for key, value in tempCollection:
            if( value < ActivityCount ):
                #remove this item the dict and add input item
                del BusyHours[key]
                BusyHours[dateTimeStamp] = ActivityCount
                break

# This function counts the number of events happened in the given interval
# TODO: The search can be optimized by persisting the last starting point of the search
def CountEventsInInterval(SortedListOfEventTimes, IntervalBegin, IntervalEnd):
    CountOfEvents = 0
    for EventTime in SortedListOfEventTimes:
        if( EventTime < IntervalBegin ):
            continue
        elif ( EventTime >= IntervalEnd ):  # as they are sorted, no need to look beyond this event
            break
        else:
            CountOfEvents += 1
    return CountOfEvents


SortedTimeStamps = sorted(NASA_FanLog.EventTimeStamps)
MinTimeStamp = SortedTimeStamps[0]
MaxTimeStamp = SortedTimeStamps[ len(SortedTimeStamps) - 1]

PrevStartIndex = 0

def GetStartIndex(StTimeInterval, SortedTimeStamps):
    for index in range(len(SortedTimeStamps[PrevStartIndex:])):
        if( SortedTimeStamps[index] >= StTimeInterval ):
            return index
    return 0
            

print("===", GetStartIndex(MinTimeStamp + timedelta(0,60), SortedTimeStamps ) )
                    
for i in range( int((MaxTimeStamp-MinTimeStamp).total_seconds()) ):
    StTimeInterval = MinTimeStamp + timedelta(0,i) # Add i seconds to first event
    EndTimeInterval = StTimeInterval + timedelta(0,60) # Add 60 seconds to start interval
    startIndex = PrevStartIndex + GetStartIndex(StTimeInterval, SortedTimeStamps[PrevStartIndex:])
    CountOfEventsInInterval = CountEventsInInterval(SortedTimeStamps[startIndex:], StTimeInterval, EndTimeInterval )
    updateTopTenBusyList(StTimeInterval, CountOfEventsInInterval)
    PrevStartIndex = startIndex            
    if ( i % 100 ) == 0 :
        print( "Processing :", i, "Prev Start Index: ", PrevStartIndex)

#Write top 10 busy intervals to the file
feature3 = open(".\\log_output\\hours.txt", "w")
ctr = 1
for BusyHour in BusyHours:
        feature3.write(str(BusyHour))
        feature3.write("\n")
        if( ctr < 10 ):
                ctr += 1
        else:
                break
feature3.close()

#Feature #4: Look for suspect windows 
    
    


    

