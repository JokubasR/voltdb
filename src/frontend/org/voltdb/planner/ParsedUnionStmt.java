/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.SelectSubqueryExpression;
import org.voltdb.planner.ParsedSelectStmt.LimitOffset;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.types.ExpressionType;

public class ParsedUnionStmt extends AbstractParsedStmt {

    public enum UnionType {
        NOUNION,
        UNION,
        UNION_ALL,
        INTERSECT,
        INTERSECT_ALL,
        EXCEPT_ALL,
        EXCEPT
    };

    // Limit plan node information.
    private LimitOffset m_limitOffset = new LimitOffset();
    // Order by
    private ArrayList<ParsedColInfo> m_orderColumns = new ArrayList<ParsedColInfo>();

    public ArrayList<AbstractParsedStmt> m_children = new ArrayList<AbstractParsedStmt>();
    public UnionType m_unionType = UnionType.NOUNION;

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedUnionStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        String type = stmtNode.attributes.get("uniontype");
        // Set operation type
        m_unionType = UnionType.valueOf(type);

        int idx = 0;
        VoltXMLElement limitElement = null, offsetElement = null, orderbyElement = null;
        for (VoltXMLElement child : stmtNode.children) {
            if (SELECT_NODE_NAME.equals(child.name) ||
                    UNION_NODE_NAME.equals(child.name)) {
                assert(idx < m_children.size());
                AbstractParsedStmt nextStmt = m_children.get(idx++);
                nextStmt.parse(child);
            } else if (child.name.equalsIgnoreCase("limit")) {
                limitElement = child;
            } else if (child.name.equalsIgnoreCase("offset")) {
                offsetElement = child;
            } else if (child.name.equalsIgnoreCase("ordercolumns")) {
                orderbyElement = child;
            }

        }
        // Parse LIMIT/OFFSET
        ParsedSelectStmt.parseLimitAndOffset(limitElement, offsetElement, m_limitOffset);
        // Parse ORDER BY
        if (orderbyElement != null) {
            parseOrderColumns(orderbyElement);
            placeTVEsForOrderby();
        }

        // prepare the limit plan node if it needs one.
        if (hasLimitOrOffset()) {
            ParsedSelectStmt.prepareLimitPlanNode(this, m_limitOffset);
        }
    }

    /**Parse tables and parameters
     *
     * @param root
     * @param db
     */
    @Override
    void parseTablesAndParams(VoltXMLElement stmtNode) {
        m_tableList.clear();
        // Parse parameters first to satisfy a dependency of expression parsing
        // which happens during table scan parsing.
        parseParameters(stmtNode);

        assert(stmtNode.children.size() > 1);
        AbstractParsedStmt childStmt = null;
        for (VoltXMLElement childSQL : stmtNode.children) {
            if (childSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                childStmt = new ParsedSelectStmt(m_paramValues, m_db);
                // Assign every child a unique ID
                childStmt.m_stmtId = AbstractParsedStmt.NEXT_STMT_ID++;
                childStmt.m_parentStmt = m_parentStmt;

            } else if (childSQL.name.equalsIgnoreCase(UNION_NODE_NAME)) {
                childStmt = new ParsedUnionStmt(m_paramValues, m_db);
                // Set the parent before recursing to children.
                childStmt.m_parentStmt = m_parentStmt;
            } else {
                // skip Order By, Limit/Offset. They will be processed later
                // by the 'parse' method
                continue;
            }
            childStmt.m_paramsById.putAll(m_paramsById);
            childStmt.parseTablesAndParams(childSQL);
            promoteUnionParametersFromChild(childStmt);
            m_children.add(childStmt);
            // Add statement's tables to the consolidated list
            m_tableList.addAll(childStmt.m_tableList);
        }
    }

    /**Miscellaneous post parse activity
     * .
     * @param sql
     * @param joinOrder
     */
    @Override
    void postParse(String sql, String joinOrder) {
        for (AbstractParsedStmt selectStmt : m_children) {
            selectStmt.postParse(sql, joinOrder);
        }

        m_sql = sql;
        m_joinOrder = joinOrder;
    }

    @Override
    public boolean isOrderDeterministic() {
        ArrayList<AbstractExpression> nonOrdered = new ArrayList<AbstractExpression>();
        return orderByColumnsDetermineAllDisplayColumns(nonOrdered);
    }

    @Override
    public boolean isOrderDeterministicInSpiteOfUnorderedSubqueries() {
        // Set OP should not have its own subqueries
        return isOrderDeterministic();
    }

    private boolean orderByColumnsDetermineAllDisplayColumns(List<AbstractExpression> nonOrdered)
    {
        return ParsedSelectStmt.orderByColumnsDetermineAllDisplayColumns(getLeftmostSelectStmt().displayColumns(), m_orderColumns, nonOrdered);
    }

    @Override
    public boolean hasLimitOrOffset() {
        return m_limitOffset.hasLimitOrOffset();
    }

    public LimitPlanNode getLimitNodeTop() {
        return m_limitOffset.getLimitNodeTop();
    }

    private void parseOrderColumns(VoltXMLElement columnsNode) {
        ParsedSelectStmt leftmostSelectChild = getLeftmostSelectStmt();
        for (VoltXMLElement child : columnsNode.children) {
            parseOrderColumn(child, leftmostSelectChild);
        }
    }

    /**
     * This is a stripped down version of the ParsedSelectStmt.parseOrderColumn. Since the SET ops
     * are not allowed to have aggregate expressions (HAVING, GROUP BY) (except the individual SELECTS)
     * all the logic handling the aggregates is omitted here
     * @param orderByNode
     * @param leftmostSelectChild
     */
    private void parseOrderColumn(VoltXMLElement orderByNode, ParsedSelectStmt leftmostSelectChild) {

        ParsedColInfo.ExpressionAdjuster adjuster = new ParsedColInfo.ExpressionAdjuster() {
            @Override
            public AbstractExpression adjust(AbstractExpression expr) {
                // Union itself can't have aggregate expression
                return expr;
            }
        };
        // Get the display columns from the first child
        List<ParsedColInfo> displayColumns = leftmostSelectChild.orderByColumns();
        ParsedColInfo order_col = ParsedColInfo.fromOrderByXml(leftmostSelectChild, orderByNode, adjuster);

        AbstractExpression order_exp = order_col.expression;
        assert(order_exp != null);

        // Mark the order by column if it is in displayColumns
        // The ORDER BY column MAY be identical to a simple display column, in which case,
        // tagging the actual display column as being also an order by column
        // helps later when trying to determine ORDER BY coverage (for determinism).
        for (ParsedColInfo col : displayColumns) {
            if (col.alias.equals(order_col.alias) || col.expression.equals(order_exp)) {
                col.orderBy = true;
                col.ascending = order_col.ascending;

                order_col.alias = col.alias;
                order_col.columnName = col.columnName;
                order_col.tableName = col.tableName;
                break;
            }
        }
        assert( ! (order_exp instanceof ConstantValueExpression));
        assert( ! (order_exp instanceof ParameterValueExpression));

        m_orderColumns.add(order_col);
    }

    /**
     * Return the leftmost child SELECT statement
     * @return ParsedSelectStmt
     */
    private ParsedSelectStmt getLeftmostSelectStmt() {
        assert (!m_children.isEmpty());
        AbstractParsedStmt firstChild = m_children.get(0);
        if (firstChild instanceof ParsedSelectStmt) {
            return (ParsedSelectStmt) firstChild;
        } else {
            assert(firstChild instanceof ParsedUnionStmt);
            return ((ParsedUnionStmt)firstChild).getLeftmostSelectStmt();
        }
    }

    @Override
    public List<ParsedColInfo> orderByColumns() {
        return Collections.unmodifiableList(m_orderColumns);
    }

    @Override
    public boolean hasOrderByColumns() {
        return ! m_orderColumns.isEmpty();
    }

    @Override
    public List<StmtSubqueryScan> findAllFromSubqueries() {
        List<StmtSubqueryScan> subqueries = new ArrayList<StmtSubqueryScan>();
        for (AbstractParsedStmt childStmt : m_children) {
            subqueries.addAll(childStmt.findAllFromSubqueries());
        }
        return subqueries;
    }

    @Override
    public List<AbstractExpression> findAllSubexpressionsOfType(ExpressionType exprType) {
        List<AbstractExpression> exprs = new ArrayList<AbstractExpression>();
        for (AbstractParsedStmt childStmt : m_children) {
            exprs.addAll(childStmt.findAllSubexpressionsOfType(exprType));
        }
        return exprs;
    }

    @Override
    public Set<AbstractExpression> findAllSubexpressionsOfClass(Class< ? extends AbstractExpression> aeClass) {
        Set<AbstractExpression> exprs = new HashSet<AbstractExpression>();
        for (AbstractParsedStmt childStmt : m_children) {
            exprs.addAll(childStmt.findAllSubexpressionsOfClass(aeClass));
        }
        return exprs;
    }

    /**
     * Break up UNION/INTERSECT (ALL) set ops into individual selects that are part
     * of the IN/EXISTS subquery into multiple expressions for each set op child
     * combined by the conjunction AND/OR expression.
     * col IN ( queryA UNION queryB ) - > col IN (queryA) OR col IN (queryB)
     * col IN ( queryA INTERSECTS queryB ) - > col IN (queryA) AND col IN (queryB)
     * The EXCEPT set op is LEFT as is
     * Also the ALL qualifier is dropped because IN/EXISTS expressions only
     * need just one tuple in the results set
     *
     * @param subqueryExpr - IN/EXISTS expression with a possible SET OP subquery
     * @return simplified expression
     */
    protected static AbstractExpression breakUpSetOpSubquery(AbstractExpression expr) {
        assert(expr != null);
        SelectSubqueryExpression subqueryExpr = null;
        if (expr.getExpressionType() == ExpressionType.COMPARE_EQUAL &&
                expr.getRight() instanceof SelectSubqueryExpression) {
            subqueryExpr = (SelectSubqueryExpression) expr.getRight();
        } else if (expr.getExpressionType() == ExpressionType.OPERATOR_EXISTS &&
                expr.getLeft() instanceof SelectSubqueryExpression) {
            subqueryExpr = (SelectSubqueryExpression) expr.getLeft();
        }
        if (subqueryExpr == null) {
            return expr;
        }
        AbstractParsedStmt subquery = subqueryExpr.getSubqueryStmt();
        if (!(subquery instanceof ParsedUnionStmt)) {
            return expr;
        }
        ParsedUnionStmt setOpStmt = (ParsedUnionStmt) subquery;
        if (UnionType.EXCEPT == setOpStmt.m_unionType || UnionType.EXCEPT_ALL == setOpStmt.m_unionType) {
            setOpStmt.m_unionType = UnionType.EXCEPT;
            return expr;
        }
        if (UnionType.UNION_ALL == setOpStmt.m_unionType) {
            setOpStmt.m_unionType = UnionType.UNION;
        } else if (UnionType.INTERSECT_ALL == setOpStmt.m_unionType) {
            setOpStmt.m_unionType = UnionType.INTERSECT;
        }
        ExpressionType conjuctionType = (setOpStmt.m_unionType == UnionType.UNION) ?
                ExpressionType.CONJUNCTION_OR : ExpressionType.CONJUNCTION_AND;
        AbstractExpression retval = null;
        AbstractParsedStmt parentStmt = subquery.m_parentStmt;
        // It's a subquery which means it must have a parent
        assert (parentStmt != null);
        for (AbstractParsedStmt child : setOpStmt.m_children) {
            // add table to the query cache
            String withoutAlias = null;
            StmtSubqueryScan tableCache = parentStmt.addSubqueryToStmtCache(child, withoutAlias);
            AbstractExpression childSubqueryExpr =
                    new SelectSubqueryExpression(subqueryExpr.getExpressionType(), tableCache);
            AbstractExpression newExpr = null;
            try {
                newExpr = expr.getExpressionType().getExpressionClass().newInstance();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage(), e);
            }
            newExpr.setExpressionType(expr.getExpressionType());
            if (ExpressionType.COMPARE_EQUAL == expr.getExpressionType()) {
                newExpr.setLeft((AbstractExpression) expr.getLeft().clone());
                newExpr.setRight(childSubqueryExpr);
                assert(newExpr instanceof ComparisonExpression);
                ((ComparisonExpression)newExpr).setQuantifier(((ComparisonExpression)expr).getQuantifier());
            } else {
                newExpr.setLeft(childSubqueryExpr);
            }
            // Recurse
            newExpr = ParsedUnionStmt.breakUpSetOpSubquery(newExpr);
            if (retval == null) {
                retval = newExpr;
            } else {
                retval = new ConjunctionExpression(conjuctionType, retval, newExpr);
            }
        }
        return retval;
    }

    private void placeTVEsForOrderby () {
        Map <AbstractExpression, Integer> displayIndexMap = new HashMap <AbstractExpression,Integer>();
        Map <Integer, ParsedColInfo> displayIndexToColumnMap = new HashMap <Integer, ParsedColInfo>();

        int orderByIndex = 0;
        ParsedSelectStmt leftmostSelectChild = getLeftmostSelectStmt();
        for (ParsedColInfo col : leftmostSelectChild.m_displayColumns) {
            displayIndexMap.put(col.expression, orderByIndex);
            assert(col.alias != null);
            displayIndexToColumnMap.put(orderByIndex, col);
            orderByIndex++;
        }

        // place the TVEs from Display columns in the ORDER BY expression
        for (ParsedColInfo orderCol : m_orderColumns) {
            AbstractExpression expr = orderCol.expression.replaceWithTVE(displayIndexMap, displayIndexToColumnMap);
            orderCol.expression = expr;
        }
    }
}
