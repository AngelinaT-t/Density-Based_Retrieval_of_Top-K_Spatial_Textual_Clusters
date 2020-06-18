package com.clusters_textual_spatial_topk;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import javax.naming.spi.DirStateFactory.Result;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import com.clusters_textual_spatial_topk.RTree;
import com.clusters_textual_spatial_topk.Constants;

import org.json.JSONException;
import org.json.JSONObject;
/*
 * 查询关键字结构体
 */
class QueryItem {
	public Point p;
	public String q;
	public int k, minpts;
	public double a, e;

	public QueryItem(Point _p, String _q, int _k, double _a, int _minpts, double _e) {
		// TODO Auto-generated constructor stub
		p = _p; // 查询点位置
		q = _q; // 查询词条
		k = _k; // 返回集群数
		a = _a; // 调节参数
		minpts = _minpts;// 设定集群最小对象数
		e = _e; // 设定邻近距离
	}
}
/*
 * 反向文件每个元素的结构体
 */
class InvertItem {
	private String index;
	private double weight;

	public InvertItem(String _index, double _weight) {
		// TODO Auto-generated constructor stub
		index = _index; //对应节点的index
		weight = _weight; // 词项在该节点对应的权重
	}

	public String getIndex() {
		return index;
	}

	public double getWeight() {
		return weight;
	}
}

public class topkSTC {

	private String foodInfoFile;
	// private String shopInfoFile;
	private double shopSum;
	private Map<String, shopInfo> shopMap;
	private BufferedReader br;
	private Directory directory;
	private StandardAnalyzer analyzer;
	private static RTree tree;
	public ArrayList<Map<String, List<InvertItem>>> InvertedFileList;
	private int index;
	private Query query;

	private IndexReader reader;
	private IndexSearcher searcher;
	private TopScoreDocCollector collector;

	private Map<String, Boolean> noise;

	/*
	 * 初始化路径等参数
	 */
	public topkSTC(String n) {
		 foodInfoFile = n;
		//foodInfoFile = "/Users/ptt/Desktop/dataset_test_3000.json";
		// shopInfoFile = "/Users/ptt/Desktop/shopInfo_final1.txt";
		shopMap = new HashMap<>();
		tree = new RTree(2, 0.4f, Constants.RTREE_QUADRATIC, 2);
		InvertedFileList = new ArrayList<Map<String, List<InvertItem>>>();
		noise = new HashMap<>();

	}

	/*
	 * 初始化每个空间文本对象
	 */
	private class shopInfo {
		private float termSum = 0.0f; // term的个数
		private Map<String, Double> TermWeight = new HashMap<>(); // 文本term的权重
	}

	/*
	 * 初始化方向文件
	 */
	private void initialInvertFile() {
		for (int i = 0; i < index; i++)
			InvertedFileList.add(new HashMap<String, List<InvertItem>>());
	}

	/*
	 * 读取文件，构建Lucene对象，分词
	 */
	private void lucenePostingList() throws IOException {
		directory = new RAMDirectory();
		analyzer = new StandardAnalyzer();
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter w = new IndexWriter(directory, config);

		String id, latitude, longitude, contents;

		try {
			br = new BufferedReader(new FileReader(foodInfoFile));// 读取原始json文件
			String str = null;
			while ((str = br.readLine()) != null) {
				// System.out.println(s);
				try {
					JSONObject json = new JSONObject(str);// 创建一个包含原始json串的json对象
					id = json.get("business_id").toString();
					latitude = json.get("latitude").toString();
					longitude = json.get("longitude").toString();
					contents = json.get("categories").toString().toLowerCase();

					if (!id.equals("null") && !latitude.equals("null") && !longitude.equals("null")
							&& !contents.equals("null")) {
						// System.out.println(id+"**" + latitude+"**" +longitude+"**" +contents);
						// 创建document对象
						Document document = new Document();

						// 创建field对象，将field添加到document对象中
						// 第一个参数：域的名称
						// 第二个参数：域的内容
						// 第三个参数：是否存储
						Field fileIDField = new TextField("id", id, Store.YES);
						Field fileLatitudeField = new TextField("latitude", latitude, Store.YES);
						Field fileLongitudeField = new TextField("longitude", longitude, Store.YES);
						Field fileContentField = new TextField("Content", contents, Store.YES);

						document.add(fileIDField);
						document.add(fileLatitudeField);
						document.add(fileLongitudeField);
						document.add(fileContentField);
						// 使用indexwriter对象将document对象写入索引库，此过程进行索引创建。并将索引和document对象写入索引库。
						w.addDocument(document);

						/**
						 * --------------------------以上Lucene索引完成，开始构建文档的term权值------------------------
						 */
						shopInfo soif = new shopInfo();
						StringTokenizer st; // 根据切词符切词的类
						String sp = "!@#$%^&*()_+`-= {}|[]\\;':\",./<>?·【】、「」|；‘：“，。／《》？1234567890 "; // 定义切词符号
						st = new StringTokenizer(contents, sp);
						while (st.hasMoreElements()) { // 遍历所有token
							soif.termSum++;
							String token = st.nextToken();
							double num = soif.TermWeight.get(token) != null ? soif.TermWeight.get(token) : 0.0;
							soif.TermWeight.put(token, ++num);
						}
						// System.out.println(soif.TermWeight);
						shopMap.put(id, soif);

						/** --------------------------以上TermWeight完成，开始构建R树------------------------ */
						Point p1 = new Point(new float[] { Float.parseFloat(latitude), Float.parseFloat(longitude) });
						final Rectangle rectangle = new Rectangle(p1, p1);
						rectangle.setId(id);
						tree.insert(rectangle);

						noise.put(id, false);
					}
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			shopSum = shopMap.size();
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// 关闭IndexWriter对象。
		w.close();
	}
	/*
	 * 计算文本term的权值
	 */
	private void calculateWeight() throws IOException, ParseException {
		for (shopInfo si : shopMap.values()) {
			Map<String, Double> tempTermWight = si.TermWeight;
			for (String t : tempTermWight.keySet()) {
				tempTermWight.put(t, tempTermWight.get(t) / si.termSum
						* (Math.log(shopSum / (luceneQueryTermHitsNum(t) + 1))) / Math.log(10));
			}
			// si.TermWeight = tempTermWight;
			// System.out.println(si.TermWeight);
		}
	}
	
	/*
	 * 构建Lucene查询结构
	 */
	private int luceneQueryTermHitsNum(String q) throws IOException, ParseException {
		query = new QueryParser("Content", analyzer).parse(q);
		int hitsPerPage = 10;
		reader = DirectoryReader.open(directory);
		searcher = new IndexSearcher(reader);
		collector = TopScoreDocCollector.create(hitsPerPage);
		searcher.search(query, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;
		// reader.close();
		return hits.length;
	}
	/*
	 * Lucene查询返回结果
	 */
	private ScoreDoc[] luceneQueryTermHitsSet(String q) throws IOException, ParseException {
		query = new QueryParser("Content", analyzer).parse(q);
		int hitsPerPage = 10;
		reader = DirectoryReader.open(directory);
		searcher = new IndexSearcher(reader);
		collector = TopScoreDocCollector.create(hitsPerPage);
		searcher.search(query, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;
		// reader.close();
		return hits;
	}

	/*
	 * 非递归遍历Rtree 设置节点index
	 */
	private List<RTNode> setRtreeNodeIndex(RTNode root) {
		if (root == null)
			throw new IllegalArgumentException("Node cannot be null.");

		List<RTNode> list = new ArrayList<RTNode>();
		List<RTNode> childrenList;

		Queue<RTNode> rq = new LinkedList<RTNode>();
		index = 0;

		if (!root.isLeaf()) {
			rq.offer(root);
			list.add(root);
			root.index = index++;
		}
		while (!rq.isEmpty()) {
			RTNode n = rq.poll();
			childrenList = ((RTDirNode) n).children;
			for (RTNode rt : childrenList) {
				if (!rt.isLeaf()) {
					rq.offer(rt);
				}
				list.add(rt);
				rt.index = index++;
			}
		}
		return list;
	}

	/*
	 * 根据权重排序
	 */
	class SortByInvertWeight implements Comparator<InvertItem> {

		@Override
		public int compare(InvertItem o1, InvertItem o2) {
			// TODO Auto-generated method stub
			if (o1.getWeight() >= o2.getWeight())
				return 1;
			else
				return -1;
		}
	}

	/*
	 * 递归遍历Rtree 构建反向文件
	 */
	private Map<String, List<InvertItem>> createInvertedFile(RTNode root) throws IOException {
		if (root == null)
			throw new IllegalArgumentException("Node cannot be null.");
		Map<String, List<InvertItem>> nodeFile = null;

		if (!root.isLeaf()) {
			nodeFile = InvertedFileList.get(root.index) != null ? InvertedFileList.get(root.index) : new HashMap<>();
			for (int i = 0; i < root.usedSpace; i++) {
				RTNode children = ((RTDirNode) root).getChild(i);
				Map<String, List<InvertItem>> childrenFile = createInvertedFile(children);

				List<InvertItem> termList = null;
				for (String key : childrenFile.keySet()) {
					termList = nodeFile.get(key) != null ? nodeFile.get(key) : new ArrayList<InvertItem>();
					double w = 0;
					for (InvertItem ii : termList) {
						if (ii.getWeight() > w)
							w = ii.getWeight();
					}
					InvertItem invertItem = new InvertItem(children.index + "", w);
					termList.add(invertItem);
					Collections.sort(termList, new SortByInvertWeight());
					nodeFile.put(key, termList);
				}

				InvertedFileList.set(root.index, nodeFile);
			}
		} else if (root.isLeaf()) {
			Rectangle[] rectangles = root.datas;
			nodeFile = new HashMap<>();
			for (Rectangle rect : rectangles) {
				if (rect != null) {
					Map<String, Double> rectTerm = shopMap.get(rect.getId()).TermWeight;
					List<InvertItem> termList = null;
					for (String key : rectTerm.keySet()) {
						termList = nodeFile.get(key) != null ? nodeFile.get(key) : new ArrayList<>();
						InvertItem invertItem = new InvertItem(rect.getId(), rectTerm.get(key));
						termList.add(invertItem);
						Collections.sort(termList, new SortByInvertWeight());
						nodeFile.put(key, termList);
					}
				}
			}
			InvertedFileList.set(root.index, nodeFile);
		}
		return nodeFile;
	}
	/*
	 * 计算两点之间的距离
	 */
	public double distance(Point p1, Point p2) {
		float x1 = p1.getFloatCoordinate(0), y1 = p1.getFloatCoordinate(1), x2 = p2.getFloatCoordinate(0),
				y2 = p2.getFloatCoordinate(1);
		return Math.sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
	}
	/*
	 * 根据距离排序
	 */
	class SortByDistance implements Comparator<Object> {

		private Point p;

		public SortByDistance(Point p) {
			// TODO Auto-generated constructor stub
			this.p = p;
		}

		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			Rectangle r1 = (Rectangle) o1, r2 = (Rectangle) o2;
			double d1 = distance(p, r1.getLow()), d2 = distance(p, r2.getLow());
			if (d1 > d2)
				return 1;
			return -1;
		}

	}
	/*
	 * 计算权重
	 */
	public double termWight(String id, String query) {
		String[] term = query.split(" |,");
		Map<String, Double> termweight = shopMap.get(id).TermWeight;
		double w = 0.0;
		for (String t : term) {
			w += (termweight.get(t) != null ? termweight.get(t) : 0);
		}
		return w;
	}
	/*
	 * 根据权重排序
	 */
	class SortByTermWeight implements Comparator<Object> {

		String query;

		public SortByTermWeight(String query) {
			// TODO Auto-generated constructor stub
			this.query = query;
		}

		@Override
		public int compare(Object o1, Object o2) {
			// TODO Auto-generated method stub
			Rectangle r1 = (Rectangle) o1, r2 = (Rectangle) o2;
			String id1 = r1.getId(), id2 = r2.getId();
			double tw1 = (1 - termWight(id1, query)), tw2 = (1 - termWight(id2, query));
			if (tw1 > tw2)
				return 1;
			return -1;
		}
	}
	/*
	 *topk-stc检索系统BASIC算法
	 */
	public List<List<Rectangle>> basic(QueryItem qq) throws IOException, ParseException {
		List<Rectangle> slist = new ArrayList<>();
		List<Rectangle> tlist = new ArrayList<>();
		ScoreDoc[] scoreDocs = luceneQueryTermHitsSet(qq.q);
		for (ScoreDoc scoreDoc : scoreDocs) {
			Document document = searcher.doc(scoreDoc.doc);
			// tlist.add(document.get("id"));
			Point p1 = new Point(new float[] { Float.parseFloat(document.get("latitude")),
					Float.parseFloat(document.get("longitude")) });
			final Rectangle rectangle = new Rectangle(p1, p1);
			rectangle.setId(document.get("id"));
			slist.add(rectangle);
			tlist.add(rectangle);
		}
		if (slist.size() != 0) {
			Collections.sort(slist, new SortByDistance(qq.p));
			Collections.sort(tlist, new SortByTermWeight(qq.q));
		} else {
			System.out.println("slist为空");
			return null;
		}

		List<List<Rectangle>> rlist = new ArrayList<>();
		int index = 0, slen = slist.size();
		double threshold = Double.POSITIVE_INFINITY, bound = 0;
		while (bound < threshold) {
			if (index >= slen)
				break;
			List<Rectangle> c1 = GetCluster(slist.get(index), qq, tlist, slist);
			if (c1 != null)
				rlist.add(c1);
			slen = slist.size();
			if (index >= slen)
				break;

			List<Rectangle> c2 = GetCluster(tlist.get(index), qq, tlist, slist);
			if (c2 != null)
				rlist.add(c2);
			slen = slist.size();
			if (index >= slen)
				break;

			if (rlist.size() >= qq.k)
				threshold = GetClsterScore(rlist.get(qq.k), qq);
			double a = qq.a;

			bound = a * distance(qq.p, slist.get(index).getLow())
					+ (1.0 - a) * (1.0 - termWight(tlist.get(index).getId(), qq.q));
			index++;
		}
		return rlist;
	}
	
	/*
	 * 计算结果簇得分
	 */
	private double GetClsterScore(List<Rectangle> set, QueryItem qq) {
		// TODO Auto-generated method stub
		double n = set.size();
		double dq = 0, tr = 0;
		String[] query = qq.q.split(" |,|&");
		for (Rectangle r : set) {
			dq += distance(r.getLow(), qq.p);
			String id = r.getId();
			Map<String, Double> tw = shopMap.get(id).TermWeight;
			for (String sq : query) {
				tr += tw.get(sq);
			}

		}
		double a = qq.a;
		return a * dq / n + (1.0 - a) * (1.0 - tr / n);
	}
	
	/*
	 * 获取核心对象的簇
	 */
	public List<Rectangle> GetCluster(Rectangle p, QueryItem qq, List<Rectangle> tlist, List<Rectangle> slist) {
		List<Rectangle> result = new ArrayList<>();
		List<Rectangle> neighbors = RangeQueue(qq, p);
		// 暂时在这里设定minpts
		int minpts = qq.minpts;
		if (neighbors.size() < minpts) {
			remove(slist, p);
			remove(tlist, p);
			noise.put(p.getId(), true);
			return null;
		} else {
			for (Rectangle r : neighbors) {
				result.add(r);
				remove(slist, r);
				remove(tlist, r);

			}
			remove(neighbors, p);
			if (neighbors.size() > 0) {
				for (int i = 0, len = neighbors.size(); i < len; i++) {
					List<Rectangle> neighborsi = RangeQueue(qq, neighbors.get(i));
					if (neighborsi.size() >= minpts) {
						for (Rectangle ri : neighborsi) {
							if (noise.get(ri.getId()))
								result.add(ri);
							else if (!result.contains(ri)) {
								result.add(ri);
								remove(slist, ri);
								remove(tlist, ri);
								neighbors.add(ri);
								len++;
							}
						}
					}
				}
			}
		}

		return result;
	}
	
	/*
	 * 从列表移除对象
	 */
	private void remove(List<Rectangle> list, Rectangle p) {
		// TODO Auto-generated method stub
		for (Rectangle r : list) {
			if (r != null && r.getId().equals(p.getId())) {
				list.remove(r);
				break;
			}

		}
	}
	
	/*
	 * 获取核心对象P的邻域
	 */
	@SuppressWarnings({ "null", "unused" })
	public List<Rectangle> RangeQueue(QueryItem qq, Rectangle p) {
		// TODO Auto-generated method stub
		List<Rectangle> neighbors = new ArrayList<Rectangle>();

		Queue<RTNode> rq = new LinkedList<RTNode>();
		Set<String> matchNode = null;
		rq.offer(tree.root);

		// 这里设定距离
		double e = qq.e;
		String[] query = qq.q.split(",| |&");

		while (!rq.isEmpty()) {
			RTNode n = rq.poll();
			if (!n.isLeaf()) {
				Map<String, List<InvertItem>> InvertedFile = InvertedFileList.get(n.index);
				matchNode = new TreeSet<>();
				for (String term : query) {
					if (InvertedFile.get(term) != null) {
						List<InvertItem> list = InvertedFile.get(term);
						for (InvertItem ii : list)
							matchNode.add(ii.getIndex());
					}
				}

				if (matchNode != null) {
					List<RTNode> childrenList = ((RTDirNode) n).children;
					Rectangle[] rectangles = n.datas;
					for (int i = 0, len = childrenList.size(); i < len; i++) {
						RTNode c = childrenList.get(i);
						if (matchNode.contains(c.index + "") && (RectangleContain(p.getLow(), rectangles[i])
								|| PointToRectangle(p.getLow(), rectangles[i]) <= e)) {
							rq.offer(c);
						}
					}
				}
			} else {
				Map<String, List<InvertItem>> InvertedFile = InvertedFileList.get(n.index);
				matchNode = new TreeSet<>();
				for (String term : query) {
					if (InvertedFile.get(term) != null) {
						List<InvertItem> list = InvertedFile.get(term);
						for (InvertItem ii : list)
							matchNode.add(ii.getIndex());
					}
				}
				Rectangle[] rectangles = n.datas;
				for (Rectangle r : rectangles) {
					if (r != null && matchNode.contains(r.getId()) && distance(r.getLow(), p.getLow()) <= e)
						neighbors.add(r);
				}
			}

		}
		return neighbors;
	}
	/*
	 * 判断点是否在MBR内
	 */
	private boolean RectangleContain(Point p, Rectangle r) {
		// TODO Auto-generated method stub
		Point low = r.getLow(), high = r.getHigh();
		float x1 = low.getFloatCoordinate(0), y1 = low.getFloatCoordinate(1), x2 = high.getFloatCoordinate(0),
				y2 = high.getFloatCoordinate(1), x = p.getFloatCoordinate(0), y = p.getFloatCoordinate(1);
		if (x >= x1 && x <= x2 && y <= y2 && y >= y1)
			return true;
		return false;
	}
	/*
	 * 判断点到MBR的距离
	 */
	private double PointToRectangle(Point p, Rectangle r) {
		// TODO Auto-generated method stub
		Point low = r.getLow(), high = r.getHigh();
		float x1 = low.getFloatCoordinate(0), y1 = low.getFloatCoordinate(1), x2 = high.getFloatCoordinate(0),
				y2 = high.getFloatCoordinate(1), x = p.getFloatCoordinate(0), y = p.getFloatCoordinate(1);
		float a1 = (y2 - y1) / (x2 - x1), b1 = y1 - a1 * x1, a2 = (y2 - y1) / (x1 - x2), b2 = y2 - a2 * x1;
		if (a1 * x + b1 == y) {
			double d1 = distance(p, low), d2 = distance(p, high);
			return d1 < d2 ? d1 : d2;
		} else if (a2 * x == y) {
			double d1 = distance(p, new Point(new float[] { x1, y2 })),
					d2 = distance(p, new Point(new float[] { x2, y1 }));
			return d1 < d2 ? d1 : d2;
		} else {
			if (x < x1)
				return x1 - x;
			else if (x > x2)
				return x - x2;
			else if (y > y2)
				return y - y2;
			else
				return y1 - y;
		}
	}

	public static void main(String[] args) throws IOException, ParseException {
		System.out.print("初始化开始");

		long start = System.currentTimeMillis(); // 获取开始时间
		topkSTC dbir = new topkSTC(args[0]);
		dbir.lucenePostingList();				//分词；Lucene处理；统计TF
		dbir.calculateWeight();					//计算term权重
		dbir.setRtreeNodeIndex(tree.root);		//设置R树index
		dbir.initialInvertFile();				//初始化反向文件
		dbir.createInvertedFile(tree.root);		//构建反向文件
		long end = System.currentTimeMillis(); 	// 获取结束时间
		System.out.print("createInvertedFile!程序运行时间： " + (end - start) + "ms");

		for (int j = 0; j < 20; j++) {
			System.out.print("查询开始");
			start = System.currentTimeMillis(); 	// 获取开始时间
			Point p = new Point(new float[] { 42.27364f, 98.8624f }); //设置查询点位置
			String q = args[1]; 		//设置各项参数
			int k = Integer.parseInt(args[2]), minpts = Integer.parseInt(args[4]);
			double a = Double.parseDouble(args[3]), e = Double.parseDouble(args[5]);
			dbir.basic(new QueryItem(p, q.toLowerCase(), k, a, minpts, e));
			//List<List<Rectangle>> result = dbir.basic(new QueryItem(p, "Tea".toLowerCase(), 10, 0.5, 20, 2));
			end = System.currentTimeMillis(); // 获取结束时间
			System.out.println(" 查询结束" + (end - start) + "ms");
		}

	}
}