package jp.co.cyberagent.valor.sdk.plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import jp.co.cyberagent.valor.sdk.plan.visitor.AttributeCollectVisitor;
import jp.co.cyberagent.valor.sdk.plan.visitor.TrimByAttributesVisitor;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.PlannerFactory;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;

public class JoinablePlanner implements Planner<ScanPlan> {

  public static final String NAME = "join";

  private final NaivePrimitivePlanner primitivePlanner = new NaivePrimitivePlanner();

  public static ScanPlan build(
      List<ProjectionItem> items, PredicativeExpression condition, Schema left, Schema right)
      throws ValorException {
    AttributeCollectVisitor collectVisitor = new AttributeCollectVisitor();
    Collection<String> predAttrs = collectVisitor.walk(condition);
    final Set<String> lks = intersect(left.getContainedAttributes(), predAttrs);
    final Set<String> rks = intersect(right.getContainedAttributes(), predAttrs);
    final Set<String> jks = intersect(lks, rks);
    Schema large;
    Schema small;
    if (lks.size() > rks.size()) {
      large = right;
      small = left;
    } else {
      large = left;
      small = right;
    }
    SchemaScan largeScan = buildTrimmedScan(condition, large);
    SchemaScan smallScan = buildTrimmedScan(condition, small);
    ScanPlan plan = buildMergeJoinPlan(items, largeScan, smallScan, condition, jks);
    if (plan != null) {
      return plan;
    }
    return new HashJoinPlan(items, largeScan, smallScan, condition, jks);
  }

  private static ScanPlan buildMergeJoinPlan(
      List<ProjectionItem> items, SchemaScan large, SchemaScan small,
      Expression filter, Set<String> joinKeys) {
    List<String> mergeJoinKeys = extractMergeJoinKey(small.getSchema(), joinKeys);
    if (mergeJoinKeys == null) {
      return null;
    }
    return new MergeJoinPlan(items, large, small, filter, joinKeys);
  }

  private static List<String> extractMergeJoinKey(Schema small, Set<String> joinKeys) {
    List<String> mergeJoinKeys = new ArrayList<>(joinKeys.size());
    Iterator<Segment> segments = small.segmentIterator();
    while (segments.hasNext()) {
      Segment segment = segments.next();
      joinKeys.stream()
          .filter(k -> !mergeJoinKeys.contains(k))
          .filter(k -> segment.containsAttribute(k))
          .forEach(k -> mergeJoinKeys.add(k));
      if (mergeJoinKeys.size() == joinKeys.size()) {
        return mergeJoinKeys;
      }
    }
    return null;
  }

  private static Set<String> intersect(Collection<String> c1, Collection<String> c2) {
    return c1.stream().filter(c2::contains).collect(Collectors.toSet());
  }

  private static SchemaScan buildTrimmedScan(PredicativeExpression condition, Schema schema)
      throws ValorException {
    Collection<String> attrs = schema.getContainedAttributes();
    PredicativeExpression q = TrimByAttributesVisitor.trim(condition, attrs);
    return schema.buildScan(attrs, q);
  }



  @Override
  public ScanPlan plan(List<ProjectionItem> items, PredicativeExpression condition,
                       Collection<Schema> schemas) throws ValorException {
    ScanPlan plan = primitivePlanner.plan(items, condition, schemas);
    if (plan != null) {
      return plan;
    }
    return enumerateJoinPlans(items, condition, schemas).stream().findFirst().get();
  }

  @Override
  public Collection<ScanPlan> enumerateAllPlans(
      List<ProjectionItem> items, PredicativeExpression conditions, Collection<Schema> schemas) {
    throw new UnsupportedOperationException();
  }

  private Collection<ScanPlan> enumerateJoinPlans(
      List<ProjectionItem> items, PredicativeExpression condition, Collection<Schema> schemas)
      throws ValorException {
    AttributeCollectVisitor visitor = new AttributeCollectVisitor();
    items.forEach(visitor::walk);
    visitor.walk(condition);
    Collection<String> attrs = visitor.getAttrributes();
    List<List<Schema>> comb = combination(schemas);
    List<ScanPlan> plans = new ArrayList<>();
    for (List<Schema> c : comb) {
      Schema left = c.get(0);
      Schema right = c.get(1);
      if (isAvailable(attrs, left, right)) {
        plans.add(build(items, condition, left, right));
      }
    }
    return plans;
  }

  @Override
  public double evaluate(ScanPlan plan) {
    // TODO implements cost function
    return 1;
  }

  private boolean isAvailable(Collection<String> attributes, Schema left, Schema right) {
    Collection<String> containedAttrs = Stream.concat(
        left.getContainedAttributes().stream(), right.getContainedAttributes().stream())
        .collect(Collectors.toSet());
    return containedAttrs.containsAll(attributes);
  }

  private List<List<Schema>> combination(Collection<Schema> schemaSet) {
    List<List<Schema>> comb = new ArrayList<>();
    Schema[] schemas = new Schema[schemaSet.size()];
    schemas = schemaSet.toArray(schemas);
    // only consider 2 element combinations
    for (int i = 0; i < schemas.length; i++) {
      for (int j = i + 1; j < schemas.length; j++) {
        comb.add(Arrays.asList(schemas[i], schemas[j]));
      }
    }
    return comb;
  }

  public static class Factory implements PlannerFactory {

    @Override
    public Planner create(ValorConf config) {
      return new JoinablePlanner();
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Planner> getProvidedClass() {
      return JoinablePlanner.class;
    }
  }
}
