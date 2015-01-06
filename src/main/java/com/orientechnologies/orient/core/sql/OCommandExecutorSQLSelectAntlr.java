package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.concur.resource.OSharedResource;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.filter.*;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLFunctionDistinct;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLFunctionCount;
import com.orientechnologies.orient.core.sql.operator.*;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.parser.OrientSqlParser;
import com.orientechnologies.orient.parser.OrientSqlParserBaseListener;
import org.antlr.v4.runtime.misc.NotNull;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// NOTE: example implementation based on OCommandExecutorSQLSelect, no manual parser method calls, just using ANTLR parser
public class OCommandExecutorSQLSelectAntlr extends OCommandExecutorSQLAntlrAbstract {
    public static final String          KEYWORD_ASC          = "ASC";
    private final OOrderByOptimizer     orderByOptimizer     = new OOrderByOptimizer();
    private final OMetricRecorder       metricRecorder       = new OMetricRecorder();
    private final OFilterOptimizer filterOptimizer      = new OFilterOptimizer();
    private final OFilterAnalyzer       filterAnalyzer       = new OFilterAnalyzer();
    private Map<String, String> projectionDefinition = null;
    // THIS HAS BEEN KEPT FOR COMPATIBILITY; BUT IT'S USED THE PROJECTIONS IN GROUPED-RESULTS
    private Map<String, Object>         projections          = null;
    private List<OPair<String, String>> orderedFields        = new ArrayList<OPair<String, String>>();
    private List<String>                groupByFields;
    private Map<Object, ORuntimeResult> groupedResult;
    private Object                      expandTarget;
    private int                         fetchLimit           = -1;
    private OIdentifiable lastRecord;
    private String                      fetchPlan;
    private volatile boolean            executing;

    private boolean                     fullySortedByIndex   = false;
    private OStorage.LOCKING_STRATEGY   lockingStrategy      = OStorage.LOCKING_STRATEGY.DEFAULT;
    private boolean                     parallel             = false;
    private Lock parallelLock         = new ReentrantLock();

    private Set<ORID> uniqueResult;

    private final class IndexUsageLog {
        OIndex<?> index;
        List<Object>     keyParams;
        OIndexDefinition indexDefinition;

        IndexUsageLog(OIndex<?> index, List<Object> keyParams, OIndexDefinition indexDefinition) {
            this.index = index;
            this.keyParams = keyParams;
            this.indexDefinition = indexDefinition;
        }
    }

    private final class IndexComparator implements Comparator<OIndex<?>> {
        public int compare(final OIndex<?> indexOne, final OIndex<?> indexTwo) {
            final OIndexDefinition definitionOne = indexOne.getDefinition();
            final OIndexDefinition definitionTwo = indexTwo.getDefinition();

            final int firstParamCount = definitionOne.getParamCount();
            final int secondParamCount = definitionTwo.getParamCount();

            final int result = firstParamCount - secondParamCount;

            if (result == 0 && !orderedFields.isEmpty()) {
                if (!(indexOne instanceof OChainedIndexProxy)
                        && orderByOptimizer.canBeUsedByOrderBy(indexOne, OCommandExecutorSQLSelectAntlr.this.orderedFields)) {
                    return 1;
                }

                if (!(indexTwo instanceof OChainedIndexProxy)
                        && orderByOptimizer.canBeUsedByOrderBy(indexTwo, OCommandExecutorSQLSelectAntlr.this.orderedFields)) {
                    return -1;
                }
            }

            return result;
        }
    }

    private static Object getIndexKey(final OIndexDefinition indexDefinition, Object value, OCommandContext context) {
        if (indexDefinition instanceof OCompositeIndexDefinition || indexDefinition.getParamCount() > 1) {
            if (value instanceof List) {
                final List<?> values = (List<?>) value;
                List<Object> keyParams = new ArrayList<Object>(values.size());

                for (Object o : values) {
                    keyParams.add(OSQLHelper.getValue(o, null, context));
                }
                return indexDefinition.createValue(keyParams);
            } else {
                value = OSQLHelper.getValue(value);
                if (value instanceof OCompositeKey) {
                    return value;
                } else {
                    return indexDefinition.createValue(value);
                }
            }
        } else {
            return indexDefinition.createValue(OSQLHelper.getValue(value));
        }
    }

    private static ODocument createIndexEntryAsDocument(final Object iKey, final OIdentifiable iValue) {
        final ODocument doc = new ODocument().setOrdered(true);
        doc.field("key", iKey);
        doc.field("rid", iValue);
        ORecordInternal.unsetDirty(doc);
        return doc;
    }

    public OCommandExecutorSQLSelectAntlr parse(final OCommandRequest iRequest) {
        super.parse(iRequest);

        addParseListener(new SelectOrientSqlParserListener());

        initContext();

        parser.parse();

        return this;
    }

    // NOTE: this listener class replaces all the manual parser method calls (at least for this class)
    protected class SelectOrientSqlParserListener extends OrientSqlParserBaseListener {
        @Override
        public void enterProjection(@NotNull OrientSqlParser.ProjectionContext ctx) {
            projections = new LinkedHashMap<String, Object>();
            projectionDefinition = new LinkedHashMap<String, String>();
        }

        @Override
        public void exitProjection(@NotNull OrientSqlParser.ProjectionContext ctx) {
            if (projectionDefinition != null && (projectionDefinition.size() > 1 || !projectionDefinition.values().iterator().next().equals("*"))) {
                projections = createProjectionFromDefinition();

                for (Object p : projections.values()) {

                    if (groupedResult == null && p instanceof OSQLFunctionRuntime && ((OSQLFunctionRuntime) p).aggregateResults()) {
                        // AGGREGATE IT
                        getProjectionGroup(null);
                        break;
                    }
                }

            } else {
                // TREATS SELECT * AS NO PROJECTION
                projectionDefinition = null;
                projections = null;
            }
        }

        @Override
        public void exitProjectionItem(@NotNull OrientSqlParser.ProjectionItemContext ctx) {
            String projection = ctx.name().getText();
            String fieldName;

            if(ctx.projectionItemAlias()!=null) {
                fieldName = ctx.projectionItemAlias().name().getText();

                if (projectionDefinition.containsKey(fieldName)) {
                    throw new OCommandSQLParsingException("Field '" + fieldName + "' is duplicated in current SELECT, choose a different name");
                }
            } else {
                fieldName = projection;

                for (int fieldIndex = 2; projectionDefinition.containsKey(fieldName); ++fieldIndex) {
                    fieldName += fieldIndex;
                }
            }

            // TODO: FLATTEN()/EXPAND() not yet implemented in parser

            // TODO: this could be done by the parser
            fieldName = OStringSerializerHelper.getStringContent(fieldName);

            projectionDefinition.put(fieldName, projection);
        }

        @Override
        public void exitFromClause(@NotNull OrientSqlParser.FromClauseContext ctx) {
            // TODO: OSQLEngine should also use the ANTLR parser infrastructure; for now just to demonstrate how code can be reduced
            // NOTE: the from clause is very simplistic at the moment (no clusters etc.)
            parsedTarget = OSQLEngine.getInstance().parseTarget(ctx.name().getText(), getContext(), KEYWORD_WHERE);
        }

        @Override
        public void exitWhereClause(@NotNull OrientSqlParser.WhereClauseContext ctx) {
            // TODO: verify this; again, the ANTLR parser could be used here
            compiledFilter = OSQLEngine.getInstance().parseCondition(ctx.getText(), getContext(), KEYWORD_WHERE);
            optimize();
        }

        @Override
        public void enterOrderBy(@NotNull OrientSqlParser.OrderByContext ctx) {
            orderedFields = new ArrayList<OPair<String, String>>();
        }

        @Override
        public void exitOrderBy(@NotNull OrientSqlParser.OrderByContext ctx) {
            if (orderedFields.size() == 0) {
                throwParsingException("Order by field set was missed. Example: ORDER BY name ASC, salary DESC");
            }
        }

        @Override
        public void exitOrderByItem(@NotNull OrientSqlParser.OrderByItemContext ctx) {
            String fieldOrdering = ctx.ordering()==null ? KEYWORD_ASC : ctx.ordering().getText();
            String fieldName = ctx.name().getText();

            orderedFields.add(new OPair<String, String>(fieldName, fieldOrdering));
        }

        @Override
        public void enterGroupBy(@NotNull OrientSqlParser.GroupByContext ctx) {
            groupByFields = new ArrayList<String>();
        }

        @Override
        public void exitGroupBy(@NotNull OrientSqlParser.GroupByContext ctx) {
            for(OrientSqlParser.NameContext nameCtx : ctx.name()) {
                groupByFields.add(nameCtx.getText());
            }

            if (groupByFields.size() == 0) {
                throwParsingException("Group by field set was missed. Example: GROUP BY name, salary");
            }

            // AGGREGATE IT
            getProjectionGroup(null);
        }

        @Override
        public void exitLock(@NotNull OrientSqlParser.LockContext ctx) {
            lockingStrategy = OStorage.LOCKING_STRATEGY.valueOf(ctx.lockStrategy().getText().toUpperCase());
        }

        @Override
        public void exitParallel(@NotNull OrientSqlParser.ParallelContext ctx) {
            parallel = true;
        }

        @Override
        public void exitFetchplan(@NotNull OrientSqlParser.FetchplanContext ctx) {
            // TODO: fetch plan could have a sub-parser; this here is very simplistic
            fetchPlan = ctx.STRING_LITERAL().getText();
            request.setFetchPlan(fetchPlan);
        }
    }

    /**
     * Determine clusters that are used in select operation
     *
     * @return set of involved cluster names
     */
    public Set<String> getInvolvedClusters() {

        final Set<String> clusters = new HashSet<String>();

        if (parsedTarget != null) {
            final ODatabaseDocument db = getDatabase();

            if (parsedTarget.getTargetQuery() != null) {
                // SUB QUERY, PROPAGATE THE CALL
                final Set<String> clIds = parsedTarget.getTargetQuery().getInvolvedClusters();
                for (String c : clIds)
                // FILTER THE CLUSTER WHERE THE USER HAS THE RIGHT ACCESS
                {
                    if (checkClusterAccess(db, c)) {
                        clusters.add(c);
                    }
                }

            } else if (parsedTarget.getTargetRecords() != null) {
                // SINGLE RECORDS: BROWSE ALL (COULD BE EXPENSIVE).
                for (OIdentifiable identifiable : parsedTarget.getTargetRecords()) {
                    final String c = db.getClusterNameById(identifiable.getIdentity().getClusterId()).toLowerCase();
                    // FILTER THE CLUSTER WHERE THE USER HAS THE RIGHT ACCESS
                    if (checkClusterAccess(db, c)) {
                        clusters.add(c);
                    }
                }
            }

            if (parsedTarget.getTargetClasses() != null) {
                return getInvolvedClustersOfClasses(parsedTarget.getTargetClasses().values());
            }

            if (parsedTarget.getTargetClusters() != null) {
                return getInvolvedClustersOfClusters(parsedTarget.getTargetClusters().values());
            }

            if (parsedTarget.getTargetIndex() != null)
            // EXTRACT THE CLASS NAME -> CLUSTERS FROM THE INDEX DEFINITION
            {
                return getInvolvedClustersOfIndex(parsedTarget.getTargetIndex());
            }

        }
        return clusters;
    }

    /**
     * @return {@code ture} if any of the sql functions perform aggregation, {@code false} otherwise
     */
    public boolean isAnyFunctionAggregates() {
        if (projections != null) {
            for (Map.Entry<String, Object> p : projections.entrySet()) {
                if (p.getValue() instanceof OSQLFunctionRuntime && ((OSQLFunctionRuntime) p.getValue()).aggregateResults()) {
                    return true;
                }
            }
        }
        return false;
    }

    public Iterator<OIdentifiable> iterator() {
        return iterator(null);
    }

    public Iterator<OIdentifiable> iterator(final Map<Object, Object> iArgs) {
        final Iterator<OIdentifiable> subIterator;
        if (target == null) {
            // GET THE RESULT
            executeSearch(iArgs);
            applyExpand();
            handleNoTarget();
            handleGroupBy();
            applyOrderBy();

            subIterator = new ArrayList<OIdentifiable>((List<OIdentifiable>) getResult()).iterator();
            lastRecord = null;
            tempResult = null;
            groupedResult = null;
        } else {
            subIterator = (Iterator<OIdentifiable>) target;
        }

        return subIterator;
    }

    public Object execute(final Map<Object, Object> iArgs) {
        try {
            bindDefaultContextVariables();

            if (iArgs != null)
            // BIND ARGUMENTS INTO CONTEXT TO ACCESS FROM ANY POINT (EVEN FUNCTIONS)
            {
                for (Map.Entry<Object, Object> arg : iArgs.entrySet()) {
                    context.setVariable(arg.getKey().toString(), arg.getValue());
                }
            }

            if (timeoutMs > 0) {
                getContext().beginExecution(timeoutMs, timeoutStrategy);
            }

            if (!optimizeExecution()) {
                fetchLimit = getQueryFetchLimit();

                executeSearch(iArgs);
                applyExpand();
                handleNoTarget();
                handleGroupBy();
                applyOrderBy();
                applyLimitAndSkip();
            }
            return getResult();
        } finally {
            if (request.getResultListener() != null) {
                request.getResultListener().end();
            }
        }
    }

    public Map<String, Object> getProjections() {
        return projections;
    }

    @Override
    public String getSyntax() {
        return "SELECT [<Projections>] FROM <Target> [LET <Assignment>*] [WHERE <Condition>*] [ORDER BY <Fields>* [ASC|DESC]*] [LIMIT <MaxRecords>] [TIMEOUT <TimeoutInMs>] [LOCK none|record]";
    }

    public String getFetchPlan() {
        return fetchPlan != null ? fetchPlan : request.getFetchPlan();
    }

    protected void executeSearch(final Map<Object, Object> iArgs) {
        assignTarget(iArgs);

        if (target == null) {
            if (let != null)
            // EXECUTE ONCE TO ASSIGN THE LET
            {
                assignLetClauses(lastRecord != null ? lastRecord.getRecord() : null);
            }

            // SEARCH WITHOUT USING TARGET (USUALLY WHEN LET/INDEXES ARE INVOLVED)
            return;
        }

        fetchFromTarget(target);
    }

    @Override
    protected boolean assignTarget(Map<Object, Object> iArgs) {
        if (!super.assignTarget(iArgs)) {
            if (parsedTarget.getTargetIndex() != null) {
                searchInIndex();
            } else {
                throw new OQueryParsingException("No source found in query: specify class, cluster(s), index or single record(s). Use "
                        + getSyntax());
            }
        }
        return true;
    }

    protected boolean executeSearchRecord(final OIdentifiable id) {
        if (Thread.interrupted()) {
            throw new OCommandExecutionException("The select execution has been interrupted");
        }

        if (!context.checkTimeout()) {
            return false;
        }

        final OStorage.LOCKING_STRATEGY contextLockingStrategy = context.getVariable("$locking") != null ? (OStorage.LOCKING_STRATEGY) context
                .getVariable("$locking") : null;

        final OStorage.LOCKING_STRATEGY localLockingStrategy = contextLockingStrategy != null ? contextLockingStrategy
                : lockingStrategy;

        ORecord record = null;
        try {
            if (id instanceof ORecord) {
                record = (ORecord) id;

                // LOCK THE RECORD IF NEEDED
                if (localLockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK) {
                    record.lock(true);
                } else if (localLockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_SHARED_LOCK) {
                    record.lock(false);
                }

            } else {
                boolean noCache = false;
                if (localLockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK
                        || localLockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_SHARED_LOCK)
                    noCache = true;
                record = getDatabase().load(id.getIdentity(), null, noCache, false, localLockingStrategy);
                if (id instanceof OContextualRecordId && ((OContextualRecordId) id).getContext() != null) {
                    Map<String, Object> ridContext = ((OContextualRecordId) id).getContext();
                    for (String key : ridContext.keySet()) {
                        context.setVariable(key, ridContext.get(key));
                    }
                }
            }

            context.updateMetric("recordReads", +1);

            if (record == null || ORecordInternal.getRecordType(record) != ODocument.RECORD_TYPE)
            // SKIP IT
            {
                return true;
            }

            context.updateMetric("documentReads", +1);

            context.setVariable("current", record);
            assignLetClauses(record);

            if (filter(record)) {
                if (!handleResult(record))
                // LIMIT REACHED
                {
                    return false;
                }
            }
        } finally {
            if (record != null) {
                if (localLockingStrategy != null)
                // CONTEXT LOCK: lock must be released (no matter if filtered or not)
                {
                    if (localLockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_EXCLUSIVE_LOCK
                            || localLockingStrategy == OStorage.LOCKING_STRATEGY.KEEP_SHARED_LOCK) {
                        record.unlock();
                    }
                }
            }
        }
        return true;
    }

    /**
     * Handles the record in result.
     *
     * @param iRecord
     *          Record to handle
     * @return false if limit has been reached, otherwise true
     */
    protected boolean handleResult(final OIdentifiable iRecord) {
        if (parallel)
        // LOCK FOR PARALLEL EXECUTION. THIS PREVENT CONCURRENT ISSUES
        {
            parallelLock.lock();
        }

        try {
            if ((orderedFields.isEmpty() || fullySortedByIndex) && skip > 0) {
                lastRecord = null;
                skip--;
                return true;
            }

            lastRecord = iRecord;

            resultCount++;

            if (!addResult(lastRecord)) {
                return false;
            }

            return !((orderedFields.isEmpty() || fullySortedByIndex) && !isAnyFunctionAggregates() && fetchLimit > -1 && resultCount >= fetchLimit);
        } finally {
            if (parallel)
            // UNLOCK PARALLEL EXECUTION
            {
                parallelLock.unlock();
            }
        }
    }

    protected boolean addResult(OIdentifiable iRecord) {
        if (iRecord == null) {
            return true;
        }

        if (projections != null || groupByFields != null && !groupByFields.isEmpty()) {
            if (groupedResult == null) {
                // APPLY PROJECTIONS IN LINE
                iRecord = ORuntimeResult.getProjectionResult(resultCount, projections, context, iRecord);
                if (iRecord == null) {
                    return true;
                }
            } else {
                // AGGREGATION/GROUP BY
                Object fieldValue = null;
                if (groupByFields != null && !groupByFields.isEmpty()) {
                    if (groupByFields.size() > 1) {
                        // MULTI-FIELD GROUP BY
                        final ODocument doc = iRecord.getRecord();
                        final Object[] fields = new Object[groupByFields.size()];
                        for (int i = 0; i < groupByFields.size(); ++i) {
                            final String field = groupByFields.get(i);
                            if (field.startsWith("$")) {
                                fields[i] = context.getVariable(field);
                            } else {
                                fields[i] = doc.field(field);
                            }
                        }
                        fieldValue = fields;
                    } else {
                        final String field = groupByFields.get(0);
                        if (field != null) {
                            if (field.startsWith("$")) {
                                fieldValue = context.getVariable(field);
                            } else {
                                fieldValue = ((ODocument) iRecord.getRecord()).field(field);
                            }
                        }
                    }
                }

                getProjectionGroup(fieldValue).applyRecord(iRecord);
                return true;
            }
        }

        boolean result = true;
        if ((fullySortedByIndex || orderedFields.isEmpty()) && expandTarget == null) {
            // SEND THE RESULT INLINE
            if (request.getResultListener() != null) {
                result = request.getResultListener().result(iRecord);
            }

        } else {

            // COLLECT ALL THE RECORDS AND ORDER THEM AT THE END
            if (tempResult == null) {
                tempResult = new ArrayList<OIdentifiable>();
            }
            ((Collection<OIdentifiable>) tempResult).add(iRecord);
        }

        return result;
    }

    protected ORuntimeResult getProjectionGroup(final Object fieldValue) {
        final long projectionElapsed = (Long) context.getVariable("projectionElapsed", 0l);
        final long begin = System.currentTimeMillis();
        try {

            Object key = null;
            if (groupedResult == null) {
                groupedResult = new LinkedHashMap<Object, ORuntimeResult>();
            }

            if (fieldValue != null) {
                if (fieldValue.getClass().isArray()) {
                    // LOOK IT BY HASH (FASTER THAN COMPARE EACH SINGLE VALUE)
                    final Object[] array = (Object[]) fieldValue;

                    final StringBuilder keyArray = new StringBuilder();
                    for (Object o : array) {
                        if (keyArray.length() > 0) {
                            keyArray.append(",");
                        }
                        if (o != null) {
                            keyArray.append(o instanceof OIdentifiable ? ((OIdentifiable) o).getIdentity().toString() : o.toString());
                        } else {
                            keyArray.append("null");
                        }
                    }

                    key = keyArray.toString();
                } else
                // LOOKUP FOR THE FIELD
                {
                    key = fieldValue;
                }
            }

            ORuntimeResult group = groupedResult.get(key);
            if (group == null) {
                group = new ORuntimeResult(fieldValue, createProjectionFromDefinition(), resultCount, context);
                groupedResult.put(key, group);
            }
            return group;
        } finally {
            context.setVariable("projectionElapsed", projectionElapsed + (System.currentTimeMillis() - begin));
        }
    }

    @Override
    protected void searchInClasses() {
        final OClass cls = parsedTarget.getTargetClasses().keySet().iterator().next();

        if (!searchForIndexes(cls)) {
            // CHECK FOR INVERSE ORDER
            final boolean browsingOrderAsc = !(orderedFields.size() == 1 && orderedFields.get(0).getKey().equalsIgnoreCase("@rid") && orderedFields
                    .get(0).getValue().equalsIgnoreCase("DESC"));
            super.searchInClasses(browsingOrderAsc);
        }
    }

    protected Map<String, Object> createProjectionFromDefinition() {
        if (projectionDefinition == null) {
            return new LinkedHashMap<String, Object>();
        }

        final Map<String, Object> projections = new LinkedHashMap<String, Object>(projectionDefinition.size());
        for (Map.Entry<String, String> p : projectionDefinition.entrySet()) {
            final Object projectionValue = OSQLHelper.parseValue(this, p.getValue(), context);
            projections.put(p.getKey(), projectionValue);
        }
        return projections;
    }

    protected boolean optimizeExecution() {
        if (compiledFilter != null) {
            mergeRangeConditionsToBetweenOperators(compiledFilter);
        }

        if ((compiledFilter == null || (compiledFilter.getRootCondition() == null)) && groupByFields == null && projections != null
                && projections.size() == 1) {

            final long startOptimization = System.currentTimeMillis();
            try {

                final Map.Entry<String, Object> entry = projections.entrySet().iterator().next();

                if (entry.getValue() instanceof OSQLFunctionRuntime) {
                    final OSQLFunctionRuntime rf = (OSQLFunctionRuntime) entry.getValue();
                    if (rf.function instanceof OSQLFunctionCount && rf.configuredParameters.length == 1
                            && "*".equals(rf.configuredParameters[0])) {

                        boolean restrictedClasses = false;
                        final OSecurityUser user = getDatabase().getUser();

                        if (parsedTarget.getTargetClasses() != null && user != null
                                && user.checkIfAllowed(ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_READ) == null) {
                            for (OClass cls : parsedTarget.getTargetClasses().keySet()) {
                                if (cls.isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME)) {
                                    restrictedClasses = true;
                                    break;
                                }
                            }
                        }

                        if (!restrictedClasses) {
                            long count = 0;

                            if (parsedTarget.getTargetClasses() != null) {
                                final OClass cls = parsedTarget.getTargetClasses().keySet().iterator().next();
                                count = cls.count();
                            } else if (parsedTarget.getTargetClusters() != null) {
                                for (String cluster : parsedTarget.getTargetClusters().keySet()) {
                                    count += getDatabase().countClusterElements(cluster);
                                }
                            } else if (parsedTarget.getTargetIndex() != null) {
                                count += getDatabase().getMetadata().getIndexManager().getIndex(parsedTarget.getTargetIndex()).getSize();
                            } else {
                                final Iterable<? extends OIdentifiable> recs = parsedTarget.getTargetRecords();
                                if (recs != null) {
                                    if (recs instanceof Collection<?>)
                                        count += ((Collection<?>) recs).size();
                                    else {
                                        for (Object o : recs)
                                            count++;
                                    }
                                }

                            }

                            if (tempResult == null)
                                tempResult = new ArrayList<OIdentifiable>();
                            ((Collection<OIdentifiable>) tempResult).add(new ODocument().field(entry.getKey(), count));
                            return true;
                        }
                    }
                }

            } finally {
                context.setVariable("optimizationElapsed", (System.currentTimeMillis() - startOptimization));
            }
        }

        return false;
    }

    protected void revertProfiler(final OCommandContext iContext, final OIndex<?> index, final List<Object> keyParams,
                                  final OIndexDefinition indexDefinition) {
        if (iContext.isRecordingMetrics()) {
            iContext.updateMetric("compositeIndexUsed", -1);
        }

        final OProfilerMBean profiler = Orient.instance().getProfiler();
        if (profiler.isRecording()) {
            profiler.updateCounter(profiler.getDatabaseMetric(index.getDatabaseName(), "query.indexUsed"), "Used index in query", -1);

            int params = indexDefinition.getParamCount();
            if (params > 1) {
                final String profiler_prefix = profiler.getDatabaseMetric(index.getDatabaseName(), "query.compositeIndexUsed");

                profiler.updateCounter(profiler_prefix, "Used composite index in query", -1);
                profiler.updateCounter(profiler_prefix + "." + params, "Used composite index in query with " + params + " params", -1);
                profiler.updateCounter(profiler_prefix + "." + params + '.' + keyParams.size(), "Used composite index in query with "
                        + params + " params and " + keyParams.size() + " keys", -1);
            }
        }
    }

    private void mergeRangeConditionsToBetweenOperators(OSQLFilter filter) {
        OSQLFilterCondition condition = filter.getRootCondition();

        OSQLFilterCondition newCondition = convertToBetweenClause(condition);
        if (newCondition != null) {
            filter.setRootCondition(newCondition);
            metricRecorder.recordRangeQueryConvertedInBetween();
            return;
        }

        mergeRangeConditionsToBetweenOperators(condition);
    }

    private void mergeRangeConditionsToBetweenOperators(OSQLFilterCondition condition) {
        if (condition == null) {
            return;
        }

        OSQLFilterCondition newCondition;

        if (condition.getLeft() instanceof OSQLFilterCondition) {
            OSQLFilterCondition leftCondition = (OSQLFilterCondition) condition.getLeft();
            newCondition = convertToBetweenClause(leftCondition);

            if (newCondition != null) {
                condition.setLeft(newCondition);
                metricRecorder.recordRangeQueryConvertedInBetween();
            } else {
                mergeRangeConditionsToBetweenOperators(leftCondition);
            }
        }

        if (condition.getRight() instanceof OSQLFilterCondition) {
            OSQLFilterCondition rightCondition = (OSQLFilterCondition) condition.getRight();

            newCondition = convertToBetweenClause(rightCondition);
            if (newCondition != null) {
                condition.setRight(newCondition);
                metricRecorder.recordRangeQueryConvertedInBetween();
            } else {
                mergeRangeConditionsToBetweenOperators(rightCondition);
            }
        }
    }

    private OSQLFilterCondition convertToBetweenClause(OSQLFilterCondition condition) {
        if (condition == null) {
            return null;
        }

        final Object right = condition.getRight();
        final Object left = condition.getLeft();

        final OQueryOperator operator = condition.getOperator();
        if (!(operator instanceof OQueryOperatorAnd)) {
            return null;
        }

        if (!(right instanceof OSQLFilterCondition)) {
            return null;
        }

        if (!(left instanceof OSQLFilterCondition)) {
            return null;
        }

        String rightField;

        final OSQLFilterCondition rightCondition = (OSQLFilterCondition) right;
        final OSQLFilterCondition leftCondition = (OSQLFilterCondition) left;

        if (rightCondition.getLeft() instanceof OSQLFilterItemField && rightCondition.getRight() instanceof OSQLFilterItemField) {
            return null;
        }

        if (!(rightCondition.getLeft() instanceof OSQLFilterItemField) && !(rightCondition.getRight() instanceof OSQLFilterItemField)) {
            return null;
        }

        if (leftCondition.getLeft() instanceof OSQLFilterItemField && leftCondition.getRight() instanceof OSQLFilterItemField) {
            return null;
        }

        if (!(leftCondition.getLeft() instanceof OSQLFilterItemField) && !(leftCondition.getRight() instanceof OSQLFilterItemField)) {
            return null;
        }

        final List<Object> betweenBoundaries = new ArrayList<Object>();

        if (rightCondition.getLeft() instanceof OSQLFilterItemField) {
            OSQLFilterItemField itemField = (OSQLFilterItemField) rightCondition.getLeft();
            if (!itemField.isFieldChain()) {
                return null;
            }

            if (itemField.getFieldChain().getItemCount() > 1) {
                return null;
            }

            rightField = itemField.getRoot();
            betweenBoundaries.add(rightCondition.getRight());
        } else if (rightCondition.getRight() instanceof OSQLFilterItemField) {
            OSQLFilterItemField itemField = (OSQLFilterItemField) rightCondition.getRight();
            if (!itemField.isFieldChain()) {
                return null;
            }

            if (itemField.getFieldChain().getItemCount() > 1) {
                return null;
            }

            rightField = itemField.getRoot();
            betweenBoundaries.add(rightCondition.getLeft());
        } else {
            return null;
        }

        betweenBoundaries.add("and");

        String leftField;
        if (leftCondition.getLeft() instanceof OSQLFilterItemField) {
            OSQLFilterItemField itemField = (OSQLFilterItemField) leftCondition.getLeft();
            if (!itemField.isFieldChain()) {
                return null;
            }

            if (itemField.getFieldChain().getItemCount() > 1) {
                return null;
            }

            leftField = itemField.getRoot();
            betweenBoundaries.add(leftCondition.getRight());
        } else if (leftCondition.getRight() instanceof OSQLFilterItemField) {
            OSQLFilterItemField itemField = (OSQLFilterItemField) leftCondition.getRight();
            if (!itemField.isFieldChain()) {
                return null;
            }

            if (itemField.getFieldChain().getItemCount() > 1) {
                return null;
            }

            leftField = itemField.getRoot();
            betweenBoundaries.add(leftCondition.getLeft());
        } else {
            return null;
        }

        if (!leftField.equalsIgnoreCase(rightField)) {
            return null;
        }

        final OQueryOperator rightOperator = ((OSQLFilterCondition) right).getOperator();
        final OQueryOperator leftOperator = ((OSQLFilterCondition) left).getOperator();

        if ((rightOperator instanceof OQueryOperatorMajor || rightOperator instanceof OQueryOperatorMajorEquals)
                && (leftOperator instanceof OQueryOperatorMinor || leftOperator instanceof OQueryOperatorMinorEquals)) {

            final OQueryOperatorBetween between = new OQueryOperatorBetween();

            if (rightOperator instanceof OQueryOperatorMajor) {
                between.setLeftInclusive(false);
            }

            if (leftOperator instanceof OQueryOperatorMinor) {
                between.setRightInclusive(false);
            }

            return new OSQLFilterCondition(new OSQLFilterItemField(this, leftField), between, betweenBoundaries.toArray());
        }

        if ((leftOperator instanceof OQueryOperatorMajor || leftOperator instanceof OQueryOperatorMajorEquals)
                && (rightOperator instanceof OQueryOperatorMinor || rightOperator instanceof OQueryOperatorMinorEquals)) {
            final OQueryOperatorBetween between = new OQueryOperatorBetween();

            if (leftOperator instanceof OQueryOperatorMajor) {
                between.setLeftInclusive(false);
            }

            if (rightOperator instanceof OQueryOperatorMinor) {
                between.setRightInclusive(false);
            }

            Collections.reverse(betweenBoundaries);

            return new OSQLFilterCondition(new OSQLFilterItemField(this, leftField), between, betweenBoundaries.toArray());

        }

        return null;
    }

    private void initContext() {
        if (context == null) {
            context = new OBasicCommandContext();
        }

        metricRecorder.setContext(context);
    }

    private void fetchFromTarget(final Iterator<? extends OIdentifiable> iTarget) {
        final long startFetching = System.currentTimeMillis();

        try {
            if (parallel) {
                parallelExec(iTarget);
            } else {
                // BROWSE, UNMARSHALL AND FILTER ALL THE RECORDS ON CURRENT THREAD
                while (iTarget.hasNext()) {
                    final OIdentifiable next = iTarget.next();
                    if (next == null)
                        break;

                    final ORID identity = next.getIdentity();

                    if (uniqueResult != null) {
                        if (uniqueResult.contains(identity))
                            continue;

                        if (identity.isValid())
                            uniqueResult.add(identity);
                    }

                    if (!executeSearchRecord(next)) {
                        break;
                    }
                }
            }

        } finally {
            context.setVariable("fetchingFromTargetElapsed", (System.currentTimeMillis() - startFetching));
        }
    }

    private void parallelExec(final Iterator<? extends OIdentifiable> iTarget) {
        final OResultSet result = (OResultSet) getResult();

        // BROWSE ALL THE RECORDS ON CURRENT THREAD BUT DELEGATE UNMARSHALLING AND FILTER TO A THREAD POOL
        final ODatabaseDocumentInternal db = getDatabase();

        if (limit > -1) {
            if (result != null) {
                result.setLimit(limit);
            }
        }

        final int cores = Runtime.getRuntime().availableProcessors();
        OLogManager.instance().debug(this, "Parallel query against %d threads", cores);

        final ThreadPoolExecutor workers = Orient.instance().getWorkers();

        executing = true;
        final List<Future<?>> jobs = new ArrayList<Future<?>>();

        // BROWSE ALL THE RECORDS AND PUT THE RECORD INTO THE QUEUE
        while (executing && iTarget.hasNext()) {
            final OIdentifiable next = iTarget.next();

            if (next == null) {
                break;
            }

            final Runnable job = new Runnable() {
                @Override
                public void run() {
                    ODatabaseRecordThreadLocal.INSTANCE.set(db);

                    if (!executeSearchRecord(next)) {
                        executing = false;
                    }
                }
            };

            jobs.add(workers.submit(job));
        }

        if (OLogManager.instance().isDebugEnabled()) {
            OLogManager.instance()
                    .debug(this, "Parallel query '%s' split in %d jobs, waiting for completion...", parserText, jobs.size());
        }

        int processed = 0;
        int total = jobs.size();
        try {
            for (Future<?> j : jobs) {
                j.get();
                processed++;

                if (OLogManager.instance().isDebugEnabled()) {
                    if (processed % (total / 10) == 0) {
                        OLogManager.instance().debug(this, "Executed parallel query %d/%d", processed, total);
                    }
                }
            }
        } catch (Exception e) {
            OLogManager.instance().error(this, "Error on executing parallel query: %s", e, parserText);
        }

        if (OLogManager.instance().isDebugEnabled()) {
            OLogManager.instance().debug(this, "Parallel query '%s' completed", parserText);
        }
    }

    private int getQueryFetchLimit() {
        final int sqlLimit;
        final int requestLimit;

        if (limit > -1) {
            sqlLimit = limit;
        } else {
            sqlLimit = -1;
        }

        if (request.getLimit() > -1) {
            requestLimit = request.getLimit();
        } else {
            requestLimit = -1;
        }

        if (sqlLimit == -1) {
            return requestLimit;
        }

        if (requestLimit == -1) {
            return sqlLimit;
        }

        return Math.min(sqlLimit, requestLimit);
    }

    private boolean tryOptimizeSort(final OClass iSchemaClass) {
        if (orderedFields.size() == 0) {
            return false;
        } else {
            return optimizeSort(iSchemaClass);
        }
    }

    @SuppressWarnings("rawtypes")
    private boolean searchForIndexes(final OClass iSchemaClass) {
        if (uniqueResult != null)
            uniqueResult.clear();

        final ODatabaseDocument database = getDatabase();
        database.checkSecurity(ORule.ResourceGeneric.CLASS, ORole.PERMISSION_READ, iSchemaClass.getName().toLowerCase());

        // fetch all possible variants of subqueries that can be used in indexes.
        if (compiledFilter == null) {
            return tryOptimizeSort(iSchemaClass);
        }

        // the main condition is a set of sub-conditions separated by OR operators
        final List<List<OIndexSearchResult>> conditionHierarchy = filterAnalyzer.analyzeMainCondition(
                compiledFilter.getRootCondition(), iSchemaClass, context);

        List<OIndexCursor> cursors = new ArrayList<OIndexCursor>();

        boolean indexIsUsedInOrderBy = false;
        List<IndexUsageLog> indexUseAttempts = new ArrayList<IndexUsageLog>();
        try {

            OIndexSearchResult lastSearchResult = null;
            for (List<OIndexSearchResult> indexSearchResults : conditionHierarchy) {
                // go through all variants to choose which one can be used for index search.
                boolean indexUsed = false;
                for (final OIndexSearchResult searchResult : indexSearchResults) {
                    lastSearchResult = searchResult;
                    final List<OIndex<?>> involvedIndexes = filterAnalyzer.getInvolvedIndexes(iSchemaClass, searchResult);

                    Collections.sort(involvedIndexes, new IndexComparator());

                    // go through all possible index for given set of fields.
                    for (final OIndex index : involvedIndexes) {
                        if (index.isRebuiding()) {
                            continue;
                        }

                        final OIndexDefinition indexDefinition = index.getDefinition();

                        if (searchResult.containsNullValues && indexDefinition.isNullValuesIgnored()) {
                            continue;
                        }

                        final OQueryOperator operator = searchResult.lastOperator;

                        // we need to test that last field in query subset and field in index that has the same position
                        // are equals.
                        if (!OIndexSearchResult.isIndexEqualityOperator(operator)) {
                            final String lastFiled = searchResult.lastField.getItemName(searchResult.lastField.getItemCount() - 1);
                            final String relatedIndexField = indexDefinition.getFields().get(searchResult.fieldValuePairs.size());
                            if (!lastFiled.equals(relatedIndexField)) {
                                continue;
                            }
                        }

                        final int searchResultFieldsCount = searchResult.fields().size();
                        final List<Object> keyParams = new ArrayList<Object>(searchResultFieldsCount);
                        // We get only subset contained in processed sub query.
                        for (final String fieldName : indexDefinition.getFields().subList(0, searchResultFieldsCount)) {
                            final Object fieldValue = searchResult.fieldValuePairs.get(fieldName);
                            if (fieldValue instanceof OSQLQuery<?>) {
                                return false;
                            }

                            if (fieldValue != null) {
                                keyParams.add(fieldValue);
                            } else {
                                if (searchResult.lastValue instanceof OSQLQuery<?>) {
                                    return false;
                                }

                                keyParams.add(searchResult.lastValue);
                            }
                        }

                        metricRecorder.recordInvolvedIndexesMetric(index);

                        OIndexCursor cursor;
                        indexIsUsedInOrderBy = orderByOptimizer.canBeUsedByOrderBy(index, orderedFields)
                                && !(index.getInternal() instanceof OChainedIndexProxy);
                        try {
                            boolean ascSortOrder = !indexIsUsedInOrderBy || orderedFields.get(0).getValue().equals(KEYWORD_ASC);

                            if (indexIsUsedInOrderBy) {
                                fullySortedByIndex = indexDefinition.getFields().size() >= orderedFields.size() && conditionHierarchy.size() == 1;
                            }

                            context.setVariable("$limit", limit);

                            cursor = operator.executeIndexQuery(context, index, keyParams, ascSortOrder);

                        } catch (OIndexEngineException e) {
                            throw e;
                        } catch (Exception e) {
                            OLogManager
                                    .instance()
                                    .error(
                                            this,
                                            "Error on using index %s in query '%s'. Probably you need to rebuild indexes. Now executing query using cluster scan",
                                            e, index.getName(), request != null && request.getText() != null ? request.getText() : "");

                            fullySortedByIndex = false;
                            cursors.clear();
                            return false;
                        }

                        if (cursor == null) {
                            continue;
                        }
                        cursors.add(cursor);
                        indexUseAttempts.add(new IndexUsageLog(index, keyParams, indexDefinition));
                        indexUsed = true;
                        break;
                    }
                    if (indexUsed) {
                        break;
                    }
                }
                if (!indexUsed) {
                    return tryOptimizeSort(iSchemaClass);
                }
            }

            if (cursors.size() == 0 || lastSearchResult == null) {
                return false;
            }
            if (cursors.size() == 1 && canOptimize(conditionHierarchy)) {
                filterOptimizer.optimize(compiledFilter, lastSearchResult);
            }

            uniqueResult = new HashSet<ORID>();
            for (OIndexCursor cursor : cursors) {
                fetchValuesFromIndexCursor(cursor);
            }
            uniqueResult.clear();
            uniqueResult = null;

            metricRecorder.recordOrderByOptimizationMetric(indexIsUsedInOrderBy, this.fullySortedByIndex);

            indexUseAttempts.clear();
            return true;
        } finally {
            for (IndexUsageLog wastedIndexUsage : indexUseAttempts) {
                revertProfiler(context, wastedIndexUsage.index, wastedIndexUsage.keyParams, wastedIndexUsage.indexDefinition);
            }
        }
    }

    private boolean canOptimize(List<List<OIndexSearchResult>> conditionHierarchy) {
        if (conditionHierarchy.size() > 1) {
            return false;
        }
        for (List<OIndexSearchResult> subCoditions : conditionHierarchy) {
            if (subCoditions.size() > 1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Use index to order documents by provided fields.
     *
     * @param iSchemaClass
     *          where search for indexes for optimization.
     * @return true if execution was optimized
     */
    private boolean optimizeSort(OClass iSchemaClass) {
        final List<String> fieldNames = new ArrayList<String>();

        for (OPair<String, String> pair : orderedFields) {
            fieldNames.add(pair.getKey());
        }

        final Set<OIndex<?>> indexes = iSchemaClass.getInvolvedIndexes(fieldNames);

        for (OIndex<?> index : indexes) {
            if (orderByOptimizer.canBeUsedByOrderBy(index, orderedFields)) {
                final boolean ascSortOrder = orderedFields.get(0).getValue().equals(KEYWORD_ASC);

                final Object key;
                if (ascSortOrder) {
                    key = index.getFirstKey();
                } else {
                    key = index.getLastKey();
                }

                if (key == null) {
                    return false;
                }

                fullySortedByIndex = true;

                if (context.isRecordingMetrics()) {
                    context.setVariable("indexIsUsedInOrderBy", true);
                    context.setVariable("fullySortedByIndex", fullySortedByIndex);

                    Set<String> idxNames = (Set<String>) context.getVariable("involvedIndexes");
                    if (idxNames == null) {
                        idxNames = new HashSet<String>();
                        context.setVariable("involvedIndexes", idxNames);
                    }

                    idxNames.add(index.getName());
                }

                final OIndexCursor cursor;
                if (ascSortOrder) {
                    cursor = index.iterateEntriesMajor(key, true, true);
                } else {
                    cursor = index.iterateEntriesMinor(key, true, false);
                }
                fetchValuesFromIndexCursor(cursor);

                return true;
            }
        }

        metricRecorder.recordOrderByOptimizationMetric(false, this.fullySortedByIndex);
        return false;
    }

    private void fetchValuesFromIndexCursor(final OIndexCursor cursor) {
        int needsToFetch;
        if (fetchLimit > 0) {
            needsToFetch = fetchLimit + skip;
        } else {
            needsToFetch = -1;
        }

        cursor.setPrefetchSize(needsToFetch);
        fetchFromTarget(cursor);
    }

    private void fetchEntriesFromIndexCursor(final OIndexCursor cursor) {
        int needsToFetch;
        if (fetchLimit > 0) {
            needsToFetch = fetchLimit + skip;
        } else {
            needsToFetch = -1;
        }

        cursor.setPrefetchSize(needsToFetch);

        Map.Entry<Object, OIdentifiable> entryRecord = cursor.nextEntry();
        if (needsToFetch > 0) {
            needsToFetch--;
        }

        while (entryRecord != null) {
            final ODocument doc = new ODocument().setOrdered(true);
            doc.field("key", entryRecord.getKey());
            doc.field("rid", entryRecord.getValue().getIdentity());
            ORecordInternal.unsetDirty(doc);

            if (!handleResult(doc))
            // LIMIT REACHED
            {
                break;
            }

            if (needsToFetch > 0) {
                needsToFetch--;
                cursor.setPrefetchSize(needsToFetch);
            }

            entryRecord = cursor.nextEntry();
        }
    }

    private boolean isRidOnlySort() {
        if (parsedTarget.getTargetClasses() != null && this.orderedFields.size() == 1
                && this.orderedFields.get(0).getKey().toLowerCase().equals("@rid")) {
            if (this.target != null && target instanceof ORecordIteratorClass) {
                return true;
            }
        }
        return false;
    }

    private void applyOrderBy() {
        if (orderedFields.isEmpty() || fullySortedByIndex || isRidOnlySort()) {
            return;
        }

        final long startOrderBy = System.currentTimeMillis();
        try {

            if (tempResult instanceof OMultiCollectionIterator) {
                final List<OIdentifiable> list = new ArrayList<OIdentifiable>();
                for (OIdentifiable o : tempResult) {
                    list.add(o);
                }
                tempResult = list;
            }

            ODocumentHelper.sort((List<? extends OIdentifiable>) tempResult, orderedFields, context);
            orderedFields.clear();

        } finally {
            metricRecorder.orderByElapsed(startOrderBy);
        }
    }

    /**
     * Extract the content of collections and/or links and put it as result
     */
    private void applyExpand() {
        if (expandTarget == null) {
            return;
        }

        final long startExpand = System.currentTimeMillis();
        try {

            if (tempResult == null) {
                tempResult = new ArrayList<OIdentifiable>();
                if (expandTarget instanceof OSQLFilterItemVariable) {
                    Object r = ((OSQLFilterItemVariable) expandTarget).getValue(null, null, context);
                    if (r != null) {
                        if (r instanceof OIdentifiable) {
                            ((Collection<OIdentifiable>) tempResult).add((OIdentifiable) r);
                        } else if (OMultiValue.isMultiValue(r)) {
                            for (Object o : OMultiValue.getMultiValueIterable(r)) {
                                ((Collection<OIdentifiable>) tempResult).add((OIdentifiable) o);
                            }
                        }
                    }
                }
            } else {
                final OMultiCollectionIterator<OIdentifiable> finalResult = new OMultiCollectionIterator<OIdentifiable>();
                finalResult.setLimit(limit);
                for (OIdentifiable id : tempResult) {
                    final Object fieldValue;
                    if (expandTarget instanceof OSQLFilterItem) {
                        fieldValue = ((OSQLFilterItem) expandTarget).getValue(id.getRecord(), null, context);
                    } else if (expandTarget instanceof OSQLFunctionRuntime) {
                        fieldValue = ((OSQLFunctionRuntime) expandTarget).getResult();
                    } else {
                        fieldValue = expandTarget.toString();
                    }

                    if (fieldValue != null) {
                        if (fieldValue instanceof ODocument) {
                            ArrayList<ODocument> partial = new ArrayList<ODocument>();
                            partial.add((ODocument) fieldValue);
                            finalResult.add(partial);
                        } else if (fieldValue instanceof Collection<?> || fieldValue.getClass().isArray() || fieldValue instanceof Iterator<?>
                                || fieldValue instanceof OIdentifiable || fieldValue instanceof ORidBag) {
                            finalResult.add(fieldValue);
                        } else if (fieldValue instanceof Map<?, ?>) {
                            finalResult.add(((Map<?, OIdentifiable>) fieldValue).values());
                        }
                    }
                }
                tempResult = finalResult;
            }
        } finally {
            context.setVariable("expandElapsed", (System.currentTimeMillis() - startExpand));
        }

    }

    private void searchInIndex() {
        final OIndex<Object> index = (OIndex<Object>) getDatabase().getMetadata().getIndexManager()
                .getIndex(parsedTarget.getTargetIndex());

        if (index == null) {
            throw new OCommandExecutionException("Target index '" + parsedTarget.getTargetIndex() + "' not found");
        }

        boolean ascOrder = true;
        if (!orderedFields.isEmpty()) {
            if (orderedFields.size() != 1) {
                throw new OCommandExecutionException("Index can be ordered only by key field");
            }

            final String fieldName = orderedFields.get(0).getKey();
            if (!fieldName.equalsIgnoreCase("key")) {
                throw new OCommandExecutionException("Index can be ordered only by key field");
            }

            final String order = orderedFields.get(0).getValue();
            ascOrder = order.equalsIgnoreCase(KEYWORD_ASC);
        }

        // nothing was added yet, so index definition for manual index was not calculated
        if (index.getDefinition() == null) {
            return;
        }

        if (compiledFilter != null && compiledFilter.getRootCondition() != null) {
            if (!"KEY".equalsIgnoreCase(compiledFilter.getRootCondition().getLeft().toString())) {
                throw new OCommandExecutionException("'Key' field is required for queries against indexes");
            }

            final OQueryOperator indexOperator = compiledFilter.getRootCondition().getOperator();

            if (indexOperator instanceof OQueryOperatorBetween) {
                final Object[] values = (Object[]) compiledFilter.getRootCondition().getRight();

                final OIndexCursor cursor = index.iterateEntriesBetween(getIndexKey(index.getDefinition(), values[0], context), true,
                        getIndexKey(index.getDefinition(), values[2], context), true, ascOrder);
                fetchEntriesFromIndexCursor(cursor);
            } else if (indexOperator instanceof OQueryOperatorMajor) {
                final Object value = compiledFilter.getRootCondition().getRight();

                final OIndexCursor cursor = index.iterateEntriesMajor(getIndexKey(index.getDefinition(), value, context), false, ascOrder);
                fetchEntriesFromIndexCursor(cursor);
            } else if (indexOperator instanceof OQueryOperatorMajorEquals) {
                final Object value = compiledFilter.getRootCondition().getRight();
                final OIndexCursor cursor = index.iterateEntriesMajor(getIndexKey(index.getDefinition(), value, context), true, ascOrder);
                fetchEntriesFromIndexCursor(cursor);

            } else if (indexOperator instanceof OQueryOperatorMinor) {
                final Object value = compiledFilter.getRootCondition().getRight();

                OIndexCursor cursor = index.iterateEntriesMinor(getIndexKey(index.getDefinition(), value, context), false, ascOrder);
                fetchEntriesFromIndexCursor(cursor);
            } else if (indexOperator instanceof OQueryOperatorMinorEquals) {
                final Object value = compiledFilter.getRootCondition().getRight();

                OIndexCursor cursor = index.iterateEntriesMinor(getIndexKey(index.getDefinition(), value, context), true, ascOrder);
                fetchEntriesFromIndexCursor(cursor);
            } else if (indexOperator instanceof OQueryOperatorIn) {
                final List<Object> origValues = (List<Object>) compiledFilter.getRootCondition().getRight();
                final List<Object> values = new ArrayList<Object>(origValues.size());
                for (Object val : origValues) {
                    if (index.getDefinition() instanceof OCompositeIndexDefinition) {
                        throw new OCommandExecutionException("Operator IN not supported yet.");
                    }

                    val = getIndexKey(index.getDefinition(), val, context);
                    values.add(val);
                }

                OIndexCursor cursor = index.iterateEntries(values, true);
                fetchEntriesFromIndexCursor(cursor);
            } else {
                final Object right = compiledFilter.getRootCondition().getRight();
                Object keyValue = getIndexKey(index.getDefinition(), right, context);
                if (keyValue == null) {
                    return;
                }

                final Object res;
                if (index.getDefinition().getParamCount() == 1) {
                    // CONVERT BEFORE SEARCH IF NEEDED
                    final OType type = index.getDefinition().getTypes()[0];
                    keyValue = OType.convert(keyValue, type.getDefaultJavaType());

                    res = index.get(keyValue);
                } else {
                    final Object secondKey = getIndexKey(index.getDefinition(), right, context);
                    if (keyValue instanceof OCompositeKey && secondKey instanceof OCompositeKey
                            && ((OCompositeKey) keyValue).getKeys().size() == index.getDefinition().getParamCount()
                            && ((OCompositeKey) secondKey).getKeys().size() == index.getDefinition().getParamCount()) {
                        res = index.get(keyValue);
                    } else {
                        OIndexCursor cursor = index.iterateEntriesBetween(keyValue, true, secondKey, true, true);
                        fetchEntriesFromIndexCursor(cursor);
                        return;
                    }

                }

                if (res != null) {
                    if (res instanceof Collection<?>) {
                        // MULTI VALUES INDEX
                        for (final OIdentifiable r : (Collection<OIdentifiable>) res) {
                            if (!handleResult(createIndexEntryAsDocument(keyValue, r.getIdentity())))
                            // LIMIT REACHED
                            {
                                break;
                            }
                        }
                    } else {
                        // SINGLE VALUE INDEX
                        handleResult(createIndexEntryAsDocument(keyValue, ((OIdentifiable) res).getIdentity()));
                    }
                }
            }

        } else {
            if (isIndexSizeQuery()) {
                getProjectionGroup(null).applyValue(projections.keySet().iterator().next(), index.getSize());
                return;
            }

            if (isIndexKeySizeQuery()) {
                getProjectionGroup(null).applyValue(projections.keySet().iterator().next(), index.getKeySize());
                return;
            }

            final OIndexInternal<?> indexInternal = index.getInternal();
            if (indexInternal instanceof OSharedResource) {
                ((OSharedResource) indexInternal).acquireExclusiveLock();
            }

            try {
                // ADD ALL THE ITEMS AS RESULT
                if (ascOrder) {
                    final Object firstKey = index.getFirstKey();
                    if (firstKey == null) {
                        return;
                    }

                    final OIndexCursor cursor = index.iterateEntriesMajor(firstKey, true, true);
                    fetchEntriesFromIndexCursor(cursor);
                } else {
                    final Object lastKey = index.getLastKey();
                    if (lastKey == null) {
                        return;
                    }

                    final OIndexCursor cursor = index.iterateEntriesMinor(lastKey, true, false);
                    fetchEntriesFromIndexCursor(cursor);
                }
            } finally {
                if (indexInternal instanceof OSharedResource) {
                    ((OSharedResource) indexInternal).releaseExclusiveLock();
                }
            }
        }
    }

    private boolean isIndexSizeQuery() {
        if (!(groupedResult != null && projections.entrySet().size() == 1)) {
            return false;
        }

        final Object projection = projections.values().iterator().next();
        if (!(projection instanceof OSQLFunctionRuntime)) {
            return false;
        }

        final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection;
        return f.getRoot().equals(OSQLFunctionCount.NAME)
                && ((f.configuredParameters == null || f.configuredParameters.length == 0) || (f.configuredParameters.length == 1 && f.configuredParameters[0]
                .equals("*")));
    }

    private boolean isIndexKeySizeQuery() {
        if (!(groupedResult != null && projections.entrySet().size() == 1)) {
            return false;
        }

        final Object projection = projections.values().iterator().next();
        if (!(projection instanceof OSQLFunctionRuntime)) {
            return false;
        }

        final OSQLFunctionRuntime f = (OSQLFunctionRuntime) projection;
        if (!f.getRoot().equals(OSQLFunctionCount.NAME)) {
            return false;
        }

        if (!(f.configuredParameters != null && f.configuredParameters.length == 1 && f.configuredParameters[0] instanceof OSQLFunctionRuntime)) {
            return false;
        }

        final OSQLFunctionRuntime fConfigured = (OSQLFunctionRuntime) f.configuredParameters[0];
        if (!fConfigured.getRoot().equals(OSQLFunctionDistinct.NAME)) {
            return false;
        }

        if (!(fConfigured.configuredParameters != null && fConfigured.configuredParameters.length == 1 && fConfigured.configuredParameters[0] instanceof OSQLFilterItemField)) {
            return false;
        }

        final OSQLFilterItemField field = (OSQLFilterItemField) fConfigured.configuredParameters[0];
        return field.getRoot().equals("key");
    }

    private void handleNoTarget() {
        if (parsedTarget == null && expandTarget == null)
        // ONLY LET, APPLY TO THEM
        {
            addResult(ORuntimeResult.createProjectionDocument(resultCount));
        }
    }

    private void handleGroupBy() {
        if (groupedResult != null && tempResult == null) {

            final long startGroupBy = System.currentTimeMillis();
            try {

                tempResult = new ArrayList<OIdentifiable>();

                for (Map.Entry<Object, ORuntimeResult> g : groupedResult.entrySet()) {
                    if (g.getKey() != null || (groupedResult.size() == 1 && groupByFields == null)) {
                        final ODocument doc = g.getValue().getResult();
                        if (doc != null && !doc.isEmpty()) {
                            ((List<OIdentifiable>) tempResult).add(doc);
                        }
                    }
                }

            } finally {
                context.setVariable("groupByElapsed", (System.currentTimeMillis() - startGroupBy));
            }
        }
    }
}
