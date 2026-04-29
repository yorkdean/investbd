"""
投资机构数据库 - 查重合并预览脚本
用法：
  python merge_preview.py          # 生成合并预览报告
  python merge_preview.py --apply  # 执行已确认的合并（需先编辑 CONFIRMED_MERGES）
"""

import sqlite3
import json
from datetime import datetime

DB_PATH = r'C:\Users\Dylan\WorkBuddy\20260423171621\investment_institutions.db'

# ============================================================
# 待确认合并列表 - 编辑此列表后运行 --apply
# 格式: {"keep": 保留的机构ID, "merge": [要合并掉的机构ID列表], "reason": "原因"}
# ============================================================
CONFIRMED_MERGES = [
    # 砺思资本 - Monolith是曾用名
    {"keep": 325, "merge": [1, 44], "reason": "Monolith砺思资本为同一家，统一为砺思资本"},
    # 武岳峰 - 同一家不同写法
    {"keep": 208, "merge": [377, 425], "reason": "武岳峰科创/资本/武岳峰为同一家"},
    # 深创投
    {"keep": 10, "merge": [169], "reason": "深创投与深创投集团为同一家"},
    # 纪源资本 - GGV是旧称
    {"keep": 189, "merge": [11], "reason": "GGV纪源资本与纪源资本为同一家"},
    # 小米系 - 小米投资/小米产投合并到小米
    {"keep": 296, "merge": [12, 426], "reason": "小米投资/小米产投合并到小米"},
    # 百度系
    {"keep": 399, "merge": [309], "reason": "百度与百度投资部为同一家"},
    # 京东系
    {"keep": 398, "merge": [308], "reason": "京东集团与京东为同一家"},
    # 字节跳动系
    {"keep": 402, "merge": [8], "reason": "字节跳动与字节跳动投资为同一家"},
    # 宁德时代系
    {"keep": 293, "merge": [13], "reason": "宁德时代与宁德时代投资为同一家"},
    # 国家电投系
    {"keep": 321, "merge": [429, 562], "reason": "国家电投相关主体合并"},
    # 软银中国
    {"keep": 384, "merge": [16], "reason": "软银中国与软银中国资本为同一家"},
    # 招银系
    {"keep": 221, "merge": [350], "reason": "招银国际资本与招银国际为同一家"},
    # 阳光融汇
    {"keep": 76, "merge": [519], "reason": "阳光融汇资本与阳光融汇为同一家"},
    # 光大控股
    {"keep": 389, "merge": [532], "reason": "光大控股与光大控股新经济为同一家"},
    # 华业天成
    {"keep": 191, "merge": [432], "reason": "华业天成资本与华业天成为同一家"},
    # 孚腾资本
    {"keep": 355, "merge": [101], "reason": "上海国投孚腾资本与孚腾资本为同一家"},
    # 国盛资本
    {"keep": 80, "merge": [273], "reason": "国盛资本与上海国盛资本为同一家"},
    # 哈勃投资
    {"keep": 14, "merge": [414], "reason": "华为哈勃投资与哈勃投资为同一家"},
    # 险峰
    {"keep": 153, "merge": [406], "reason": "险峰与险峰K2VC为同一家"},
    # 新尚资本
    {"keep": 559, "merge": [452], "reason": "新尚资本与无锡新尚资本为同一家"},
    # 康桥资本
    {"keep": 312, "merge": [557], "reason": "康桥资本与康桥资本CBC Group为同一家"},
    # 初心资本 vs 心资本（心资本名称模糊，合并到初心资本）
    {"keep": 167, "merge": [445], "reason": "心资本名称模糊，合并到初心资本"},
]

# ============================================================
# 所有疑似重复组（自动检测）
# ============================================================
SIMILAR_PAIRS = [
    (308, 398, '京东集团', '京东'),
    (296, 426, '小米', '小米产投'),
    (12, 296, '小米投资', '小米'),
    (309, 399, '百度', '百度投资部'),
    (10, 169, '深创投', '深创投集团'),
    (153, 406, '险峰', '险峰K2VC'),
    (14, 414, '华为哈勃投资', '哈勃投资'),
    (80, 273, '国盛资本', '上海国盛资本'),
    (8, 402, '字节跳动投资', '字节跳动'),
    (13, 293, '宁德时代投资', '宁德时代'),
    (16, 384, '软银中国资本', '软银中国'),
    (389, 532, '光大控股', '光大控股新经济'),
    (101, 355, '上海国投孚腾资本', '孚腾资本'),
    (429, 562, '国家电投', '国家电投产业基金'),
    (321, 429, '国家电投创新投资', '国家电投'),
    (191, 432, '华业天成资本', '华业天成'),
    (208, 425, '武岳峰科创', '武岳峰'),
    (377, 425, '武岳峰资本', '武岳峰'),
    (1, 325, 'Monolith 砺思资本', '砺思资本'),
    (44, 325, 'Monolith砺思资本', '砺思资本'),
    (452, 559, '新尚资本', '无锡新尚资本'),
    (221, 350, '招银国际资本', '招银国际'),
    (76, 519, '阳光融汇资本', '阳光融汇'),
    (11, 189, 'GGV纪源资本', '纪源资本'),
    (312, 557, '康桥资本', '康桥资本 CBC Group'),
    (167, 445, '初心资本', '心资本'),
    (134, 445, '鼎心资本', '心资本'),
]


def get_inst_info(cur, inst_id):
    """获取机构详细信息和标签"""
    cur.execute("SELECT * FROM investment_institutions WHERE id=?", (inst_id,))
    row = cur.fetchone()
    if not row:
        return None
    inst = dict(row)
    
    cur.execute("""
        SELECT year, list_name, rank, source_org, category 
        FROM institution_tags 
        WHERE institution_id=? 
        ORDER BY year DESC, list_name
    """, (inst_id,))
    inst['tags'] = [dict(r) for r in cur.fetchall()]
    inst['tag_count'] = len(inst['tags'])
    
    return inst


def print_inst_comparison(inst_a, inst_b):
    """打印两家机构的对比信息"""
    print(f"  {'✅' if inst_a['tag_count'] > inst_b['tag_count'] else '  '} "
          f"[{inst_a['id']:3d}] {inst_a['name']}")
    print(f"        类型: {inst_a.get('type') or '-':20s} "
          f"规模: {inst_a.get('fund_scale') or '-':15s} "
          f"标签: {inst_a['tag_count']}条")
    if inst_a.get('focus_areas'):
        print(f"        领域: {inst_a['focus_areas'][:50]}")
    if inst_a.get('cases'):
        print(f"        案例: {inst_a['cases'][:50]}")
    
    print(f"  {'✅' if inst_b['tag_count'] > inst_a['tag_count'] else '  '} "
          f"[{inst_b['id']:3d}] {inst_b['name']}")
    print(f"        类型: {inst_b.get('type') or '-':20s} "
          f"规模: {inst_b.get('fund_scale') or '-':15s} "
          f"标签: {inst_b['tag_count']}条")
    if inst_b.get('focus_areas'):
        print(f"        领域: {inst_b['focus_areas'][:50]}")
    if inst_b.get('cases'):
        print(f"        案例: {inst_b['cases'][:50]}")


def generate_preview_report():
    """生成合并预览报告"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    print("=" * 90)
    print("投资机构数据库 - 查重合并预览报告")
    print(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90)
    print()
    
    # 按机构分组（同一个机构可能在多组中出现）
    groups = []
    processed = set()
    
    for id_a, id_b, name_a, name_b in SIMILAR_PAIRS:
        inst_a = get_inst_info(cur, id_a)
        inst_b = get_inst_info(cur, id_b)
        if not inst_a or not inst_b:
            continue
        
        # 判断建议操作
        tags_a = inst_a['tag_count']
        tags_b = inst_b['tag_count']
        
        if tags_a == 0 and tags_b == 0:
            suggestion = "❓ 两家都无标签，建议确认是否同一家"
            action = "confirm"
        elif tags_a > 0 and tags_b > 0:
            suggestion = "⚠️  两家都有标签，需手动选择保留哪边"
            action = "choose"
        else:
            # 一方有标签，一方没有
            if tags_a >= tags_b:
                keep_id, merge_id = id_a, id_b
                keep_name, merge_name = name_a, name_b
            else:
                keep_id, merge_id = id_b, id_a
                keep_name, merge_name = name_b, name_a
            suggestion = f"✅ 建议合并: 将 [{merge_id}] 合并到 [{keep_id}]"
            action = "auto"
        
        groups.append({
            'id_a': id_a, 'id_b': id_b,
            'name_a': name_a, 'name_b': name_b,
            'inst_a': inst_a, 'inst_b': inst_b,
            'suggestion': suggestion,
            'action': action,
            'keep_id': id_a if tags_a >= tags_b else id_b,
            'merge_id': id_b if tags_a >= tags_b else id_a,
        })
    
    # 打印报告
    auto_merge = [g for g in groups if g['action'] == 'auto']
    choose_merge = [g for g in groups if g['action'] == 'choose']
    confirm_merge = [g for g in groups if g['action'] == 'confirm']
    
    print(f"共发现 {len(groups)} 组疑似重复")
    print(f"  ✅ 可自动合并（一方无标签）: {len(auto_merge)} 组")
    print(f"  ⚠️  需手动选择: {len(choose_merge)} 组")
    print(f"  ❓ 需确认是否同一家: {len(confirm_merge)} 组")
    print()
    
    # 可自动合并的组
    if auto_merge:
        print("-" * 90)
        print("【可自动合并】以下组一方无标签或有少量标签，建议直接合并")
        print("-" * 90)
        for i, g in enumerate(auto_merge, 1):
            print(f"\n[{i}] {g['name_a']} <=> {g['name_b']}")
            print_inst_comparison(g['inst_a'], g['inst_b'])
            print(f"  👉 {g['suggestion']}")
            print(f"     合并命令: merge {g['merge_id']} -> {g['keep_id']}")
    
    # 需手动选择的组
    if choose_merge:
        print()
        print("-" * 90)
        print("【需手动选择】以下组两家都有标签数据，请选择保留哪一家")
        print("-" * 90)
        for i, g in enumerate(choose_merge, 1):
            print(f"\n[{i}] {g['name_a']} <=> {g['name_b']}")
            print_inst_comparison(g['inst_a'], g['inst_b'])
            print(f"  ⚠️  {g['suggestion']}")
            print(f"     请选择保留 [{g['id_a']}] 还是 [{g['id_b']}]")
    
    # 需确认的组
    if confirm_merge:
        print()
        print("-" * 90)
        print("【需确认是否同一家】以下组两家都无标签数据")
        print("-" * 90)
        for i, g in enumerate(confirm_merge, 1):
            print(f"\n[{i}] {g['name_a']} <=> {g['name_b']}")
            print_inst_comparison(g['inst_a'], g['inst_b'])
            print(f"  ❓ {g['suggestion']}")
    
    # 生成合并配置
    print()
    print("=" * 90)
    print("【下一步】")
    print("=" * 90)
    print("1. 编辑本脚本中的 CONFIRMED_MERGES 列表")
    print("2. 对每个需要合并的组，指定:")
    print("   - keep: 保留的机构 ID（通常是有更多标签的那家）")
    print("   - merge: 要合并掉的机构 ID 列表")
    print("   - reason: 合并原因")
    print("3. 运行: python merge_preview.py --apply")
    print()
    
    # 自动生成 CONFIRMED_MERGES 配置
    print("参考配置（复制到 CONFIRMED_MERGES 并修改）:")
    print("-" * 90)
    config_lines = []
    for g in auto_merge:
        config_lines.append(
            f"    {{\"keep\": {g['keep_id']}, \"merge\": [{g['merge_id']}], "
            f"\"reason\": \"{g['name_a']}/{g['name_b']} 名称统一\"}},")
    if config_lines:
        print("CONFIRMED_MERGES = [")
        for line in config_lines:
            print(line)
        print("]")
    else:
        print("(无可自动合并的组，需手动编辑 CONFIRMED_MERGES)")
    
    conn.close()


def merge_institutions(cur, keep_id, merge_ids, reason):
    """将多个机构合并到一个机构"""
    for merge_id in merge_ids:
        # 1. 获取保留方和合并方的信息
        cur.execute("SELECT name FROM investment_institutions WHERE id=?", (keep_id,))
        keep_name = cur.fetchone()[0]
        cur.execute("SELECT name FROM investment_institutions WHERE id=?", (merge_id,))
        merge_name = cur.fetchone()[0]
        
        # 2. 合并标签（避免重复）
        cur.execute("""
            SELECT year, list_name, rank, source_org, category 
            FROM institution_tags 
            WHERE institution_id=?
        """, (merge_id,))
        merge_tags = cur.fetchall()
        
        added = 0
        skipped = 0
        for tag in merge_tags:
            # 检查保留方是否已有相同标签
            cur.execute("""
                SELECT COUNT(*) FROM institution_tags 
                WHERE institution_id=? AND year=? AND list_name=? AND source_org=?
            """, (keep_id, tag[0], tag[1], tag[3]))
            if cur.fetchone()[0] == 0:
                cur.execute("""
                    INSERT INTO institution_tags (institution_id, year, list_name, rank, source_org, category)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (keep_id, tag[0], tag[1], tag[2], tag[3], tag[4]))
                added += 1
            else:
                skipped += 1
        
        # 3. 合并其他字段（如果保留方为空，用合并方的数据填充）
        cur.execute("SELECT * FROM investment_institutions WHERE id=?", (keep_id,))
        keep_inst = dict(cur.fetchone())
        cur.execute("SELECT * FROM investment_institutions WHERE id=?", (merge_id,))
        merge_inst = dict(cur.fetchone())
        
        update_fields = {}
        for field in ['type', 'fund_scale', 'focus_areas', 'cases', 'notes']:
            if not keep_inst.get(field) and merge_inst.get(field):
                update_fields[field] = merge_inst[field]
        
        if update_fields:
            set_clause = ', '.join(f'{k}=?' for k in update_fields)
            cur.execute(f"""
                UPDATE investment_institutions 
                SET {set_clause}
                WHERE id=?
            """, list(update_fields.values()) + [keep_id])
        
        # 4. 删除被合并的机构
        cur.execute("DELETE FROM investment_institutions WHERE id=?", (merge_id,))
        
        # 5. 更新保留方的 tag_count 和年份范围
        cur.execute("SELECT COUNT(*) FROM institution_tags WHERE institution_id=?", (keep_id,))
        new_tag_count = cur.fetchone()[0]
        cur.execute("SELECT MIN(year), MAX(year) FROM institution_tags WHERE institution_id=? AND year IS NOT NULL", (keep_id,))
        year_range = cur.fetchone()
        
        update_sql = "UPDATE investment_institutions SET tag_count=?"
        params = [new_tag_count]
        if year_range[0]:
            update_sql += ", first_year=?, last_year=?"
            params.extend([year_range[0], year_range[1]])
        update_sql += " WHERE id=?"
        params.append(keep_id)
        cur.execute(update_sql, params)
        
        print(f"  ✅ 已合并 [{merge_id}] {merge_name} -> [{keep_id}] {keep_name}")
        print(f"     转移标签: {added} 条（跳过重复: {skipped} 条）")
        if update_fields:
            print(f"     补充字段: {', '.join(update_fields.keys())}")


def apply_confirmed_merges():
    """执行已确认的合并"""
    if not CONFIRMED_MERGES:
        print("❌ CONFIRMED_MERGES 为空，请先编辑脚本中的合并列表")
        return
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    
    print("=" * 90)
    print("执行确认的合并操作")
    print("=" * 90)
    print()
    
    for i, merge_task in enumerate(CONFIRMED_MERGES, 1):
        keep_id = merge_task['keep']
        merge_ids = merge_task['merge']
        reason = merge_task.get('reason', '')
        
        print(f"[{i}] 合并: {merge_ids} -> {keep_id}")
        print(f"    原因: {reason}")
        print()
        
        merge_institutions(cur, keep_id, merge_ids, reason)
        print()
    
    conn.commit()
    print("=" * 90)
    print(f"✅ 完成 {len(CONFIRMED_MERGES)} 个合并任务")
    print("=" * 90)
    
    # 显示合并后的统计
    cur.execute("SELECT COUNT(*) FROM investment_institutions")
    print(f"当前机构总数: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM institution_tags")
    print(f"当前标签总数: {cur.fetchone()[0]}")
    
    conn.close()


if __name__ == '__main__':
    import sys
    if '--apply' in sys.argv:
        apply_confirmed_merges()
    else:
        generate_preview_report()
