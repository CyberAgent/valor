package jp.co.cyberagent.valor.hbase.optimize;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.FixedLengthHolder;
import jp.co.cyberagent.valor.sdk.holder.SuffixHolder;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.visitor.AttributeCollectVisitor;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.optimize.Enumerator;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.FromClause;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.plan.model.RelationSource;
import jp.co.cyberagent.valor.spi.plan.model.UnaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import org.apache.commons.collections4.iterators.PermutationIterator;

public class HBaseEnumerator implements Enumerator {

  public static final String STORAGE_CLASSNAME
      = "jp.co.cyberagent.valor.hbase.storage.HBaseStorage";

  @Override
  public List<SchemaDescriptor> enumerateSchemasForScan(RelationScan query, ValorConf storageConf) {
    List<SchemaDescriptor> schemas = new ArrayList<>();
    for (List<PrimitivePredicate> predicates : enumeratePredicateCombinations(query)) {
      FromClause from = query.getFrom();
      if (!(from instanceof RelationSource)) {
        throw new UnsupportedOperationException("only relation source is supported, but" + from);
      }
      Relation relation = ((RelationSource) from).getRelation();
      schemas.add(createSchemaFromAttributes(relation, predicates, query, storageConf));
    }
    return schemas;
  }

  private List<List<PrimitivePredicate>> enumeratePredicateCombinations(RelationScan query) {
    OrOperator condition = query.getCondition().getDnf();
    Map<String, PrimitivePredicate> conditionByAttr = null;
    for (PredicativeExpression conjunction : condition) {
      Map<String, PrimitivePredicate> tmpConditionByAttr
          = groupPredicateByAttr((AndOperator) conjunction);
      conditionByAttr = unionPredicates(conditionByAttr, tmpConditionByAttr);
    }
    return ImmutableList.copyOf(new PermutationIterator(conditionByAttr.values()));
  }

  private Map<String, PrimitivePredicate> groupPredicateByAttr(AndOperator conjunction) {
    Map<String, PrimitivePredicate> result = new HashMap<>();
    for (PredicativeExpression predicate : conjunction) {
      String attr = extractAttribute((PrimitivePredicate) predicate);
      if (attr == null) {
        continue;
      }
      if (compareStrength((PrimitivePredicate) predicate, result.get(attr)) > 0) {
        result.put(attr, (PrimitivePredicate) predicate);
      }
    }
    return result;
  }

  private Map<String, PrimitivePredicate> unionPredicates(
      Map<String, PrimitivePredicate> c1, Map<String, PrimitivePredicate> c2) {
    if (c1 == null) {
      return c2;
    }
    Collection<String> keys = new HashSet<>(c1.keySet());
    keys.addAll(c2.keySet());
    return keys.stream().collect(Collectors.toMap(
        k -> k,
        k -> {
          PrimitivePredicate p1 = c1.get(k);
          PrimitivePredicate p2 = c2.get(k);
          return compareStrength(p1, p2) > 0 ? p2 : p1;
        }
    ));
  }

  private String extractAttribute(PrimitivePredicate predicate) {
    if (predicate instanceof BinaryPrimitivePredicate) {
      BinaryPrimitivePredicate bop = (BinaryPrimitivePredicate) predicate;
      return bop.getAttributeIfUnaryPredicate();
    }
    if (predicate instanceof UnaryPrimitivePredicate) {
      UnaryPrimitivePredicate uop = (UnaryPrimitivePredicate) predicate;
      Expression operand = uop.getOperand();
      if (operand instanceof AttributeNameExpression) {
        return ((AttributeNameExpression) operand).getName();
      }
    }
    return null;
  }

  /**
   * @return negative if 1st argument is weaker than 2nd one, otherwize positiv
   */
  private int compareStrength(PrimitivePredicate p1, PrimitivePredicate p2) {
    if (p1 == null) {
      return -1;
    }
    if (p1 instanceof EqualOperator) {
      return 1;
    }
    if (p1 instanceof GreaterthanOperator || p1 instanceof GreaterthanorequalOperator
        || p1 instanceof LessthanOperator || p1 instanceof LessthanorequalOperator) {
      return p2 instanceof EqualOperator ? -1 : 1;
    }
    return p2 == null ? 1 : -1;
  }

  public SchemaDescriptor createSchemaFromAttributes(Relation relation,
                                                     List<PrimitivePredicate> predicates,
                                                     RelationScan query,
                                                     ValorConf storageConf) {
    AttributeCollectVisitor attrCollector = new AttributeCollectVisitor();
    query.accept(attrCollector);
    final Set<String> attrNames = attrCollector.getAttrributes();
    // allocate attributes used in condition to rowkey
    List<Segment> rowKeys = new ArrayList<>();
    for (PrimitivePredicate predicate : predicates) {
      if (predicate instanceof BinaryPrimitivePredicate) {
        BinaryPrimitivePredicate bop = (BinaryPrimitivePredicate) predicate;
        String attr = bop.getAttributeIfUnaryPredicate();
        if (attr == null) {
          throw new IllegalArgumentException("attribute is not found in " + predicate);
        }
        rowKeys.add(deriveSegment(bop, relation));
        attrNames.remove(attr);
      } else {
        throw new IllegalArgumentException("unexpected predicate " + predicate);
      }
    }
    final String schemaId = "s_" + Arrays.hashCode(rowKeys.toArray());
    List<Segment> values = new ArrayList<>();
    for (String attrName : attrNames) {
      Relation.Attribute attr = relation.getAttribute(attrName);
      Formatter formatter = AttributeValueFormatter.create(attrName);
      int size = attr.type().getSize();
      Segment segment = size > 0
          ? FixedLengthHolder.create(size, formatter) : VintSizePrefixHolder.create(formatter);
      if (relation.isKey(attrName)) {
        rowKeys.add(segment);
      } else {
        values.add(segment);
      }
    }

    SchemaDescriptor descriptor =
        ImmutableSchemaDescriptor.builder()
            .schemaId(schemaId)
            .storageClassName(STORAGE_CLASSNAME)
            .storageConf(storageConf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create("tbl")))
            .addField(HBaseCell.ROWKEY, rowKeys)
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create("f")))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(ConstantFormatter.create("a1")))
            .addField(HBaseCell.VALUE, values)
            .build();
    return descriptor;
  }

  private Segment deriveSegment(BinaryPrimitivePredicate predicate, Relation relation) {
    String attrName = predicate.getAttributeIfUnaryPredicate();
    if (attrName == null) {
      throw new IllegalArgumentException("attribute is not found in predicate " + predicate);
    }
    Relation.Attribute attr = relation.getAttribute(attrName);
    if (attr == null) {
      throw new IllegalArgumentException(
          "attribute " + attrName + " is not found in relation " + relation.getRelationId());
    }
    Formatter formatter = AttributeValueFormatter.create(attrName);
    boolean fixedLength = attr.type().getSize() > 0;
    if (fixedLength) {
      return FixedLengthHolder.create(attr.type().getSize(), formatter);
    }
    if (predicate instanceof EqualOperator) {
      return VintSizePrefixHolder.create(formatter);
    } else {
      // TODO make delimiter configurable
      return SuffixHolder.create("\u001f", formatter);
    }
  }
}
