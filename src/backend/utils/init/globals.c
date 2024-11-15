/*-------------------------------------------------------------------------
 *
 * globals.c
 *	  global variable declarations
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/init/globals.c
 *
 * NOTES
 *	  Globals used all over the place should be declared here and not
 *	  in other modules.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/time.h>

#include "common/file_perm.h"
#include "libpq/libpq-be.h"
#include "libpq/pqcomm.h"
#include "miscadmin.h"
#include "storage/backendid.h"
#include "utils/timestamp.h"

ProtocolVersion FrontendProtocol;

/* --------------------------------------- #RAIN -------------------------------------- */

// 去掉首尾空白字符
char* trim_token(char* str) 
{
    char* end;

    while (isspace((unsigned char)*str)) str++;

    if (*str == 0)
        return str;

    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;

    *(end + 1) = '\0';

    return str;
}

int getExprEvalOpValue(const char* op_name) 
{
    if (strcmp(op_name, "EEOP_DONE") == 0) return 0;
    if (strcmp(op_name, "EEOP_INNER_FETCHSOME") == 0) return 1;
    if (strcmp(op_name, "EEOP_OUTER_FETCHSOME") == 0) return 2;
    if (strcmp(op_name, "EEOP_SCAN_FETCHSOME") == 0) return 3;
    if (strcmp(op_name, "EEOP_INNER_VAR") == 0) return 4;
    if (strcmp(op_name, "EEOP_OUTER_VAR") == 0) return 5;
    if (strcmp(op_name, "EEOP_SCAN_VAR") == 0) return 6;
    if (strcmp(op_name, "EEOP_INNER_SYSVAR") == 0) return 7;
    if (strcmp(op_name, "EEOP_OUTER_SYSVAR") == 0) return 8;
    if (strcmp(op_name, "EEOP_SCAN_SYSVAR") == 0) return 9;
    if (strcmp(op_name, "EEOP_WHOLEROW") == 0) return 10;
    if (strcmp(op_name, "EEOP_ASSIGN_INNER_VAR") == 0) return 11;
    if (strcmp(op_name, "EEOP_ASSIGN_OUTER_VAR") == 0) return 12;
    if (strcmp(op_name, "EEOP_ASSIGN_SCAN_VAR") == 0) return 13;
    if (strcmp(op_name, "EEOP_ASSIGN_TMP") == 0) return 14;
    if (strcmp(op_name, "EEOP_ASSIGN_TMP_MAKE_RO") == 0) return 15;
    if (strcmp(op_name, "EEOP_CONST") == 0) return 16;
    if (strcmp(op_name, "EEOP_FUNCEXPR") == 0) return 17;
    if (strcmp(op_name, "EEOP_FUNCEXPR_STRICT") == 0) return 18;
    if (strcmp(op_name, "EEOP_FUNCEXPR_FUSAGE") == 0) return 19;
    if (strcmp(op_name, "EEOP_FUNCEXPR_STRICT_FUSAGE") == 0) return 20;
    if (strcmp(op_name, "EEOP_BOOL_AND_STEP_FIRST") == 0) return 21;
    if (strcmp(op_name, "EEOP_BOOL_AND_STEP") == 0) return 22;
    if (strcmp(op_name, "EEOP_BOOL_AND_STEP_LAST") == 0) return 23;
    if (strcmp(op_name, "EEOP_BOOL_OR_STEP_FIRST") == 0) return 24;
    if (strcmp(op_name, "EEOP_BOOL_OR_STEP") == 0) return 25;
    if (strcmp(op_name, "EEOP_BOOL_OR_STEP_LAST") == 0) return 26;
    if (strcmp(op_name, "EEOP_BOOL_NOT_STEP") == 0) return 27;
    if (strcmp(op_name, "EEOP_QUAL") == 0) return 28;
    if (strcmp(op_name, "EEOP_JUMP") == 0) return 29;
    if (strcmp(op_name, "EEOP_JUMP_IF_NULL") == 0) return 30;
    if (strcmp(op_name, "EEOP_JUMP_IF_NOT_NULL") == 0) return 31;
    if (strcmp(op_name, "EEOP_JUMP_IF_NOT_TRUE") == 0) return 32;
    if (strcmp(op_name, "EEOP_NULLTEST_ISNULL") == 0) return 33;
    if (strcmp(op_name, "EEOP_NULLTEST_ISNOTNULL") == 0) return 34;
    if (strcmp(op_name, "EEOP_NULLTEST_ROWISNULL") == 0) return 35;
    if (strcmp(op_name, "EEOP_NULLTEST_ROWISNOTNULL") == 0) return 36;
    if (strcmp(op_name, "EEOP_BOOLTEST_IS_TRUE") == 0) return 37;
    if (strcmp(op_name, "EEOP_BOOLTEST_IS_NOT_TRUE") == 0) return 38;
    if (strcmp(op_name, "EEOP_BOOLTEST_IS_FALSE") == 0) return 39;
    if (strcmp(op_name, "EEOP_BOOLTEST_IS_NOT_FALSE") == 0) return 40;
    if (strcmp(op_name, "EEOP_PARAM_EXEC") == 0) return 41;
    if (strcmp(op_name, "EEOP_PARAM_EXTERN") == 0) return 42;
    if (strcmp(op_name, "EEOP_PARAM_CALLBACK") == 0) return 43;
    if (strcmp(op_name, "EEOP_CASE_TESTVAL") == 0) return 44;
    if (strcmp(op_name, "EEOP_MAKE_READONLY") == 0) return 45;
    if (strcmp(op_name, "EEOP_IOCOERCE") == 0) return 46;
    if (strcmp(op_name, "EEOP_DISTINCT") == 0) return 47;
    if (strcmp(op_name, "EEOP_NOT_DISTINCT") == 0) return 48;
    if (strcmp(op_name, "EEOP_NULLIF") == 0) return 49;
    if (strcmp(op_name, "EEOP_SQLVALUEFUNCTION") == 0) return 50;
    if (strcmp(op_name, "EEOP_CURRENTOFEXPR") == 0) return 51;
    if (strcmp(op_name, "EEOP_NEXTVALUEEXPR") == 0) return 52;
    if (strcmp(op_name, "EEOP_ARRAYEXPR") == 0) return 53;
    if (strcmp(op_name, "EEOP_ARRAYCOERCE") == 0) return 54;
    if (strcmp(op_name, "EEOP_ROW") == 0) return 55;
    if (strcmp(op_name, "EEOP_ROWCOMPARE_STEP") == 0) return 56;
    if (strcmp(op_name, "EEOP_ROWCOMPARE_FINAL") == 0) return 57;
    if (strcmp(op_name, "EEOP_MINMAX") == 0) return 58;
    if (strcmp(op_name, "EEOP_FIELDSELECT") == 0) return 59;
    if (strcmp(op_name, "EEOP_FIELDSTORE_DEFORM") == 0) return 60;
    if (strcmp(op_name, "EEOP_FIELDSTORE_FORM") == 0) return 61;
    if (strcmp(op_name, "EEOP_SBSREF_SUBSCRIPT") == 0) return 62;
    if (strcmp(op_name, "EEOP_SBSREF_OLD") == 0) return 63;
    if (strcmp(op_name, "EEOP_SBSREF_ASSIGN") == 0) return 64;
    if (strcmp(op_name, "EEOP_SBSREF_FETCH") == 0) return 65;
    if (strcmp(op_name, "EEOP_DOMAIN_TESTVAL") == 0) return 66;
    if (strcmp(op_name, "EEOP_DOMAIN_NOTNULL") == 0) return 67;
    if (strcmp(op_name, "EEOP_DOMAIN_CHECK") == 0) return 68;
    if (strcmp(op_name, "EEOP_CONVERT_ROWTYPE") == 0) return 69;
    if (strcmp(op_name, "EEOP_SCALARARRAYOP") == 0) return 70;
    if (strcmp(op_name, "EEOP_XMLEXPR") == 0) return 71;
    if (strcmp(op_name, "EEOP_AGGREF") == 0) return 72;
    if (strcmp(op_name, "EEOP_GROUPING_FUNC") == 0) return 73;
    if (strcmp(op_name, "EEOP_WINDOW_FUNC") == 0) return 74;
    if (strcmp(op_name, "EEOP_SUBPLAN") == 0) return 75;
    if (strcmp(op_name, "EEOP_ALTERNATIVE_SUBPLAN") == 0) return 76;
    if (strcmp(op_name, "EEOP_AGG_STRICT_DESERIALIZE") == 0) return 77;
    if (strcmp(op_name, "EEOP_AGG_DESERIALIZE") == 0) return 78;
    if (strcmp(op_name, "EEOP_AGG_STRICT_INPUT_CHECK_ARGS") == 0) return 79;
    if (strcmp(op_name, "EEOP_AGG_STRICT_INPUT_CHECK_NULLS") == 0) return 80;
    if (strcmp(op_name, "EEOP_AGG_PLAIN_PERGROUP_NULLCHECK") == 0) return 81;
    if (strcmp(op_name, "EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL") == 0) return 82;
    if (strcmp(op_name, "EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL") == 0) return 83;
    if (strcmp(op_name, "EEOP_AGG_PLAIN_TRANS_BYVAL") == 0) return 84;
    if (strcmp(op_name, "EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF") == 0) return 85;
    if (strcmp(op_name, "EEOP_AGG_PLAIN_TRANS_STRICT_BYREF") == 0) return 86;
    if (strcmp(op_name, "EEOP_AGG_PLAIN_TRANS_BYREF") == 0) return 87;
    if (strcmp(op_name, "EEOP_AGG_ORDERED_TRANS_DATUM") == 0) return 88;
    if (strcmp(op_name, "EEOP_AGG_ORDERED_TRANS_TUPLE") == 0) return 89;
	if (strcmp(op_name, "EEOP_LAST") == 0) return 90;
    return -1;  // 返回无效值表示未知的操作码
}

/********************************** #RAIN : LLVM - ME *********************************/

volatile	int 						Reserved1       =   0;
volatile	int 						Reserved2       =   0;
volatile	int 						Reserved3       =   0;

volatile	char*						databaseName    =   NULL;
volatile	int 						planNum         =   0;

volatile	char* 						currentStatement        =   NULL;
volatile	int							currentStatementExecNum =   0;

volatile	int 						allExprNum              =   0;
volatile	int* 						allExprList             =   NULL;
volatile	int* 						currentExprJITLevels    =   NULL;

volatile	int 						previousLevelsNum       =   0;
volatile	long long* 					previousExprJITLevels   =   NULL;

volatile	int 						exprNum         =   0;
volatile	struct 		ExprSteps*	    exprTypes       =   NULL;

volatile	int 						allOptInfoNum       =   0;
volatile	struct 	    OperatorInfo*	currentSQLOpts      =   NULL;
volatile	int 						allExprInfoNum      =   0;
volatile	struct 	    ExprInfo*	    currentSQLExprs     =   NULL;

volatile	int 						currentLevelsCount  =   0;

volatile	bool 						enableQuartet   =   false;
volatile	bool 						enableME        =   false;
volatile	bool 						containExpr     =   false;
volatile	bool 						printExpr       =   false;
volatile	bool 						printExprLevels =   false;
volatile	bool 						printJITExpr    =   false;
volatile	bool 						printEEOP       =   false;
volatile	bool 						countExpr       =   false;

/************************* #RAIN : PC3 TRANSACTION PREDICTION ************************/

volatile	int 	        kValue	            =	2;
volatile	int 	        index_current_txn	=	-1;	
volatile    struct          MySharedData*		sharedData; 
volatile    struct          TransactionPool* 	transactionPool;
volatile    struct          CommitTxnPool*		commitTxnPool;		
// volatile    struct          PC3Cache* 			predictionData;     // Markov
// volatile    struct          PC3Cache* 			predictionData2;    // LSTM
// volatile    struct          PC3Cache* 			predictionData3;    // Transformer

/*************************************************************************************/

ExprInfo* 
initializeExprInfo(long long eHash, int stepLength, int* steps)
{
    ExprInfo* temp		  =	 (ExprInfo*)malloc(sizeof(ExprInfo));
    temp->expression_id   =   0;
    temp->hash            =   eHash;
    temp->step_lens       =   stepLength;
    temp->count           =   0;
    for(int i=0; i<stepLength; i++)
    {   
        temp->steps[i] = steps[i];
    }
    temp->next            =   NULL;
    return temp;
}

void  
initializeOperatorInfo(OperatorInfo* temp, int type, double	startup_cost, double total_cost)
{
    temp->optId                 =   0;
    temp->type                  =   type;
    temp->startup_cost          =   startup_cost;
    temp->total_cost            =   total_cost;
    temp->actual_input_rows     =   0.0;
    temp->actual_output_rows    =   0.0; 
    temp->filtered_rows         =   0.0;
    temp->loops                 =   0.0;
    temp->actual_startup_time   =   0.0;
    temp->actual_total_time     =   0.0;
    temp->left                  =   NULL;
    temp->right                 =   NULL;
    temp->next                  =   NULL;
}

OperatorInfo* 
findOperatorInfo(int optId, int type, double startup_cost, double total_cost)
{
    for(int i=0; i<allOptInfoNum; i++)
    {
        if(currentSQLOpts[i].optId == optId && currentSQLOpts[i].type == type && currentSQLOpts[i].startup_cost == startup_cost && currentSQLOpts[i].total_cost == total_cost)
		{
            return &currentSQLOpts[i];
        }
    }

    return NULL;
}


ExprSteps* 
initializeExprStepsList()
{
	ExprSteps* temp			=	(ExprSteps*)malloc(EXPR_LIST_SIZE * sizeof(ExprSteps));
    
	for(int i=0; i<EXPR_LIST_SIZE; i++)
    {
        temp[i].eIndex          =   i;
        temp[i].expression_id   =   0;
        temp[i].hash            =   0LL;
        temp[i].step_lens       =   0;
        temp[i].next            =   NULL;
    }
    return      temp;
}


// 哈希函数，用于计算给定 EEOP 步骤数组的哈希值
long long calculate_hash(int step_lens, int* steps) 
{
    long long hash = 17; 

    for (int i = 0; i < step_lens; i++) 
    {
        hash = (hash * 3) + steps[i]; // hash * 3 + steps[i]
    }

    return hash;
}

// 查找表达式列表中是否已有相同步骤的表达式
ExprSteps* find_expression(int step_lens, int* steps) 
{
    if(!exprTypes || exprNum == 0)
        return NULL;

    long long    eHash       =   calculate_hash(step_lens, steps);

    int     tIndex      =   (int)(eHash % (long long)EXPR_LIST_SIZE);
    int     initIndex   =   tIndex;

    while(exprTypes[tIndex].hash != 0LL)
    {
        if (exprTypes[tIndex].hash == eHash && exprTypes[tIndex].step_lens == step_lens)  
        {
            return &exprTypes[tIndex];
        }

        tIndex++;

        if(tIndex == EXPR_LIST_SIZE) tIndex = 0;

        if(tIndex == initIndex) break;
    }

    return NULL;
}

// 向表达式列表中添加新的表达式
ExprSteps* add_expression(int step_lens, int* steps) 
{
    ExprSteps* existing_expr = find_expression(step_lens, steps);

    if (!existing_expr) 
    {
        long long eHash     =   calculate_hash(step_lens, steps);

        int     tIndex      =   (int)(eHash % (long long)EXPR_LIST_SIZE);
        int     initIndex   =   tIndex;

        while(exprTypes[tIndex].hash != 0LL)
        {
            if (exprTypes[tIndex].hash == eHash && exprTypes[tIndex].step_lens == step_lens)  
            {
                return &exprTypes[tIndex];
            }

            tIndex++;

            if(tIndex == EXPR_LIST_SIZE) tIndex = 0;

            if(tIndex == initIndex) break;
        }

        if(exprTypes[tIndex].hash == 0LL)
        {
            exprTypes[tIndex].eIndex            =   tIndex;
            exprTypes[tIndex].expression_id     =   ++exprNum;
            exprTypes[tIndex].step_lens         =   step_lens;
            exprTypes[tIndex].hash              =   eHash;

            if(printJITExpr)
            {
                printf("[Add Expr] ID = %d, steps_len = %d, hash = %ld\n", exprNum+1, step_lens, eHash);
            }

            for(int i=0; i<step_lens; i++)
            {
                exprTypes[tIndex].steps[i]  =   steps[i];
            }

            return &exprTypes[tIndex];
        }
    }

    return  existing_expr;
}

/*************************************************************************************/


volatile sig_atomic_t InterruptPending = false;
volatile sig_atomic_t QueryCancelPending = false;
volatile sig_atomic_t ProcDiePending = false;
volatile sig_atomic_t ClientConnectionLost = false;
volatile sig_atomic_t IdleInTransactionSessionTimeoutPending = false;
volatile sig_atomic_t ProcSignalBarrierPending = false;
volatile uint32 InterruptHoldoffCount = 0;
volatile uint32 QueryCancelHoldoffCount = 0;
volatile uint32 CritSectionCount = 0;

int			MyProcPid;
pg_time_t	MyStartTime;
TimestampTz MyStartTimestamp;
struct Port *MyProcPort;
int32		MyCancelKey;
int			MyPMChildSlot;

/*
 * MyLatch points to the latch that should be used for signal handling by the
 * current process. It will either point to a process local latch if the
 * current process does not have a PGPROC entry in that moment, or to
 * PGPROC->procLatch if it has. Thus it can always be used in signal handlers,
 * without checking for its existence.
 */
struct Latch *MyLatch;

/*
 * DataDir is the absolute path to the top level of the PGDATA directory tree.
 * Except during early startup, this is also the server's working directory;
 * most code therefore can simply use relative paths and not reference DataDir
 * explicitly.
 */
char	   *DataDir = NULL;

/*
 * Mode of the data directory.  The default is 0700 but it may be changed in
 * checkDataDir() to 0750 if the data directory actually has that mode.
 */
int			data_directory_mode = PG_DIR_MODE_OWNER;

char		OutputFileName[MAXPGPATH];	/* debugging output file */

char		my_exec_path[MAXPGPATH];	/* full path to my executable */
char		pkglib_path[MAXPGPATH]; /* full path to lib directory */

#ifdef EXEC_BACKEND
char		postgres_exec_path[MAXPGPATH];	/* full path to backend */

/* note: currently this is not valid in backend processes */
#endif

BackendId	MyBackendId = InvalidBackendId;

BackendId	ParallelMasterBackendId = InvalidBackendId;

Oid			MyDatabaseId = InvalidOid;

Oid			MyDatabaseTableSpace = InvalidOid;

/*
 * DatabasePath is the path (relative to DataDir) of my database's
 * primary directory, ie, its directory in the default tablespace.
 */
char	   *DatabasePath = NULL;

pid_t		PostmasterPid = 0;

/*
 * IsPostmasterEnvironment is true in a postmaster process and any postmaster
 * child process; it is false in a standalone process (bootstrap or
 * standalone backend).  IsUnderPostmaster is true in postmaster child
 * processes.  Note that "child process" includes all children, not only
 * regular backends.  These should be set correctly as early as possible
 * in the execution of a process, so that error handling will do the right
 * things if an error should occur during process initialization.
 *
 * These are initialized for the bootstrap/standalone case.
 */
bool		IsPostmasterEnvironment = false;
bool		IsUnderPostmaster = false;
bool		IsBinaryUpgrade = false;
bool		IsBackgroundWorker = false;

bool		ExitOnAnyError = false;

int			DateStyle = USE_ISO_DATES;
int			DateOrder = DATEORDER_MDY;
int			IntervalStyle = INTSTYLE_POSTGRES;

bool		enableFsync = true;
bool		allowSystemTableMods = false;
int			work_mem = 1024;
double		hash_mem_multiplier = 1.0;
int			maintenance_work_mem = 16384;
int			max_parallel_maintenance_workers = 2;

/*
 * Primary determinants of sizes of shared-memory structures.
 *
 * MaxBackends is computed by PostmasterMain after modules have had a chance to
 * register background workers.
 */
int			NBuffers = 1000;
int			MaxConnections = 90;
int			max_worker_processes = 8;
int			max_parallel_workers = 8;
int			MaxBackends = 0;

int			VacuumCostPageHit = 1;	/* GUC parameters for vacuum */
int			VacuumCostPageMiss = 10;
int			VacuumCostPageDirty = 20;
int			VacuumCostLimit = 200;
double		VacuumCostDelay = 0;

int64		VacuumPageHit = 0;
int64		VacuumPageMiss = 0;
int64		VacuumPageDirty = 0;

int			VacuumCostBalance = 0;	/* working state for vacuum */
bool		VacuumCostActive = false;

double		vacuum_cleanup_index_scale_factor;
