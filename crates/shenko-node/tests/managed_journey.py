#!/usr/bin/env python3
"""Two real isolated journeys, not physical two-machine or LLM evidence.
Run with python3 -I SCRIPT REPO NODE_BINARY SSH_DRIVER SSHD LIB_DIR NEW_ROOT.
Only creates/controls a new loopback SSH fixture and private per-node stores.
"""
import base64
import copy
import hashlib
import gzip
import json
import hashlib
import os
from pathlib import Path
import runpy
import select
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

repo, binary, ssh_driver, sshd, libdir, root = map(Path, sys.argv[1:7])
legacy_source = Path(sys.argv[7]) if len(sys.argv) == 8 else None
root.mkdir(mode=0o700)
d = runpy.run_path(str(repo / 'scripts/managed_node.py'))
node = lambda directory, request: d['call_binary'](binary, directory, request)
uid = lambda: str(uuid.uuid4())

# One bounded fault lifecycle at the Python publication primitive, NOT SSH or
# power-loss evidence. Never interpret a direct primitive re-entry as C07 replay.
def publication_fault():
    folder=root/'publication-fault';folder.mkdir(mode=0o700)
    raw=b'bounded publication fixture, not a native runtime'
    chunk=folder/'chunk';chunk.write_bytes(gzip.compress(raw,mtime=0))
    sha=hashlib.sha256(raw).hexdigest();interrupted=uid()
    prefix="""import json,os
 def interrupted_dump(value,file,*args,**kwargs):
  file.write('{"interrupted":');file.flush();os.fsync(file.fileno());os._exit(91)
 json.dump=interrupted_dump
""".replace('\n ', '\n')
    argv=[str(folder),interrupted,sha,str(chunk)]
    crash=subprocess.run([sys.executable,'-I','-c',prefix+d['INSTALL'],*argv],capture_output=True)
    assert crash.returncode==91,crash.stderr
    assert not (folder/('install-'+interrupted+'.json')).exists()
    assert list(folder.glob('install-'+interrupted+'.json.prepared-*.new'))
    # A pre-existing damaged final is preserved, not silently overwritten or
    # treated as durable. Repair is an explicit NEW install identity/operation.
    damaged=folder/('install-'+interrupted+'.json')
    damaged.write_bytes(b'{"interrupted":')
    rejected=subprocess.run([sys.executable,'-I','-c',d['INSTALL'],*argv],capture_output=True)
    assert rejected.returncode!=0 and damaged.read_bytes()==b'{"interrupted":'
    repair=uid()
    completed=subprocess.run([sys.executable,'-I','-c',d['INSTALL'],str(folder),repair,sha,str(chunk)],capture_output=True)
    assert completed.returncode==0,completed.stderr
    receipt=json.loads(completed.stdout)
    assert json.loads((folder/('install-'+repair+'.json')).read_bytes())==receipt
    assert damaged.read_bytes()==b'{"interrupted":' and receipt['operation_id']!=interrupted
    final=folder/('install-'+repair+'.json')
    (folder/'valid-receipt.preserved').write_bytes(final.read_bytes())
    final.write_text(json.dumps(dict(receipt,state='conflicting')))
    conflicting=final.read_bytes()
    rejected=subprocess.run([sys.executable,'-I','-c',d['INSTALL'],str(folder),repair,sha,str(chunk)],capture_output=True)
    assert rejected.returncode!=0 and final.read_bytes()==conflicting
    return {'kind':'pure_python_process_fault','crash_exit':91,'partial_final_published':False,
        'damaged_evidence_preserved':True,'repair_operation':repair,'C07_redispatch_test':False,'power_loss_test':False}
publication=publication_fault()
print(json.dumps({'publication':publication}),flush=True)
identity = {'agent_id': uid(), 'branch_id': uid(), 'origin_proof': 'sha256:' + '1'*64}
old_self = {'agent_id': uid(), 'branch_id': uid(), 'origin_proof': 'sha256:' + '2'*64}
controller_id, worker_id = uid(), uid()
controller = root / 'controller'
node(controller, {'command':'initialize', 'node_id':controller_id, 'identity':identity})
for name in ['host_key', 'client_key']:
    subprocess.run(['ssh-keygen','-q','-t','ed25519','-N','','-f',str(root/name)],check=True)
(root/'authorized_keys').write_bytes((root/'client_key.pub').read_bytes())
def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0));return s.getsockname()[1]
server_port=port()
listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen()
proxy_port=listener.getsockname()[1]
config=root/'sshd_config'
config.write_text(f"Port {server_port}\nListenAddress 127.0.0.1\nHostKey {root}/host_key\nPidFile {root}/sshd.pid\nAuthorizedKeysFile {root}/authorized_keys\nPasswordAuthentication no\nKbdInteractiveAuthentication no\nPermitRootLogin prohibit-password\nStrictModes no\nUsePAM no\nAllowUsers root\nLogLevel ERROR\n")
public=(root/'host_key.pub').read_text().split()
(root/'known_hosts').write_text(f'[127.0.0.1]:{proxy_port} {public[0]} {public[1]}\n')
remote_root=root/'remote-worker'
# The proxy really closes an SSH stream after the managed install receipt is
# durable. Delay only that stream's packets so the observation cannot race past
# the receipt marker; subsequent receipt queries use a healthy new connection.
dropped=threading.Event();stopping=threading.Event()
marker=[None]
relays=[]
def installation_ready():
    if marker[0] is None:return False
    plan,operation=marker[0]
    try:
        install=json.loads((remote_root/('install-'+plan['deployment_id']+'.json')).read_bytes())
        expected={'operation_id':plan['deployment_id'],'sha256':plan['artifact_sha256'],
            'artifact':str(remote_root/('artifact-'+plan['deployment_id'])),
            'state':'installed','service':'one_shot_no_daemon'}
        if install!=expected:return False
        if hashlib.sha256(Path(install['artifact']).read_bytes()).hexdigest()!=plan['artifact_sha256']:return False
        receipt=json.loads((root/'receipts'/operation/'receipt.json').read_bytes())
        return (receipt['operation_id']==operation and receipt['state']=='completed' and receipt['exit_code']==0
            and not receipt['stdout_truncated'] and not receipt['stderr_truncated']
            and json.loads(base64.b64decode(receipt['stdout_b64']))==install)
    except (OSError,ValueError,KeyError):return False

def relay(client):
    upstream=None
    try:
        upstream=socket.create_connection(('127.0.0.1',server_port))
        while not stopping.is_set():
            readers,_,_=select.select([client,upstream],[],[],0.05)
            for src in readers:
                data=src.recv(262144)
                if not data:return
                if src is upstream and marker[0] is not None and not dropped.is_set():
                    time.sleep(0.04)
                    if installation_ready():dropped.set();return
                (upstream if src is client else client).sendall(data)
    except (OSError,ConnectionError):pass
    finally:
        client.close()
        if upstream is not None:upstream.close()
def serve():
    listener.settimeout(0.1)
    while not stopping.is_set():
        try:client,_=listener.accept()
        except socket.timeout:continue
        except OSError:return
        worker=threading.Thread(target=relay,args=(client,),daemon=True)
        relays.append(worker);worker.start()
log=open(root/'sshd.log','xb')
server=subprocess.Popen([str(sshd),'-D','-e','-f',str(config)],stdout=log,stderr=log,env=dict(os.environ,LD_LIBRARY_PATH=str(libdir)))
proxy=threading.Thread(target=serve,daemon=True);proxy.start()
try:
    for _ in range(100):
        if server.poll() is not None:raise RuntimeError('sshd failed; inspect private fixture log')
        try:
            with socket.create_connection(('127.0.0.1',server_port),timeout=0.1):break
        except OSError:time.sleep(0.02)
    ctx={'task_id':uid(),'task_revision':1,'record_revision':0,'invocation_id':uid(),'epoch':1,'worker_id':uid(),'node_id':controller_id}
    parent={'db':str(root/'effects.sqlite3'),'cas':str(root/'effects-objects'),'context':ctx,'operation_id':uid(),'logical_key':'fixture-only-parent',
        'target':{'host':'127.0.0.1','port':proxy_port,'user':'root','key_path':str(root/'client_key'),'known_hosts_path':str(root/'known_hosts'),'use_agent':False},
        'target_id':'c17-owned-loopback-node','receipt_root':str(root/'receipts'),'action':{'kind':'exec','argv':['true'],'stdin_b64':''},'timeout_secs':15}
    d['dump'](root/'parent.json',parent)
    init=subprocess.run([str(ssh_driver),'fixture-init',str(root/'parent.json')],capture_output=True)
    assert init.returncode==0,init.stderr
    config={'artifact':str(binary),'controller_binary':str(binary),'controller_data':str(controller),'node_id':worker_id,
        'remote_root':str(remote_root),'parent':parent,'identity':identity,'initial_identity':old_self,'ssh_driver':str(ssh_driver)}
    d['dump'](root/'config.json',config)
    plan_dir=root/'deployment'
    d['prepare'](plan_dir,root/'config.json')
    plan=json.loads((plan_dir/'plan.json').read_text())
    op=str(uuid.uuid5(uuid.UUID(plan['deployment_id']),'install'))
    # Journey 1 includes ownership refusal and the actual v1 -> v2 install
    # migration. The legacy source is read-only; all migration happens on backup.
    unrelated=root/'unrelated-data';unrelated.mkdir(mode=0o700)
    with sqlite3.connect(unrelated/'node.sqlite3') as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('CREATE TABLE unrelated(secret TEXT)')
        db.execute("INSERT INTO unrelated VALUES('preserve-me')")
    before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in unrelated.iterdir()}
    rejected=subprocess.run([str(binary),str(unrelated)],input=b'{"command":"status"}',capture_output=True)
    assert rejected.returncode!=0 and b'unrecognized node installation' in rejected.stderr
    assert before=={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in unrelated.iterdir()}
    migration=None
    if legacy_source:
        migrated=root/'migrated-installation';migrated.mkdir(mode=0o700)
        with sqlite3.connect('file:'+str(legacy_source/'node.sqlite3')+'?mode=ro&immutable=1',uri=True) as old:
            assert old.execute('PRAGMA user_version').fetchone()[0]==1
            tables=[r[0] for r in old.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name")]
            rows={t:old.execute('SELECT * FROM '+t+' ORDER BY rowid').fetchall() for t in tables}
            with sqlite3.connect(migrated/'node.sqlite3') as backup:old.backup(backup)
        migrated_status=node(migrated,{'command':'status'})
        with sqlite3.connect(migrated/'node.sqlite3') as db:
            assert db.execute('PRAGMA user_version').fetchone()[0]==2
            assert db.execute('PRAGMA application_id').fetchone()[0]==0x53484e4b
            for t,old_rows in rows.items():
                new_rows=db.execute('SELECT * FROM '+t+' ORDER BY rowid').fetchall()
                if t=='meta':new_rows=[r for r in new_rows if r[0]!='installation']
                assert new_rows==old_rows,t
        assert node(migrated,{'command':'status'})['metadata']==migrated_status['metadata']
        migration={'storage_schema':2,'old_tables_preserved':len(tables),'source_readonly':True}
    marker[0]=(plan,op)
    # Interleave A and B on the SAME registered target/root with different ELF
    # bytes. A must survive B's stage and then retry every upload without replay.
    d['stage'](plan_dir,plan)
    variant=root/'shenko-node-B'
    variant.write_bytes(binary.read_bytes()+b'\nC17 distinct deployment artifact B\n')
    variant.chmod(0o700)
    config_b=dict(config,artifact=str(variant))
    d['dump'](root/'config-B.json',config_b)
    other_dir=root/'deployment-B'
    d['prepare'](other_dir,root/'config-B.json')
    other=json.loads((other_dir/'plan.json').read_text())
    assert other['artifact_sha256']!=plan['artifact_sha256']
    assert other['staging_root']!=plan['staging_root']
    d['stage'](other_dir,other)
    for candidate in [plan,other]:
        for chunk,sha in candidate['chunks_sha256'].items():
            assert hashlib.sha256((Path(candidate['staging_root'])/chunk).read_bytes()).hexdigest()==sha
    # Journey 1: real artifact upload/install -> lost SSH response -> original
    # receipt query -> resume/adopt existing self -> capability/status readback.
    try:d['deploy'](plan_dir,plan)
    except RuntimeError as error:assert 'unknown' in str(error),str(error)
    else:raise AssertionError('response-loss injection did not interrupt install')
    assert dropped.is_set() and installation_ready()
    # Local file removal or unrelated trust edits cannot erase the frozen
    # operation's outcome. They still prevent a NEW admission under this plan.
    known_hosts=root/'known_hosts'
    trust=known_hosts.read_bytes()
    known_hosts.rename(root/'known_hosts.preserved')
    recovered=d['invoke'](plan_dir,plan,'install',query=True)
    assert recovered['state']=='installed'
    try:d['invoke'](plan_dir,plan,'new-with-missing-trust',d['action'](['true']))
    except FileNotFoundError:pass
    else:raise AssertionError('new admission accepted missing trust')
    known_hosts.write_bytes(trust+b'unrelated.invalid '+public[0].encode()+b' '+public[1].encode()+b'\n')
    assert d['invoke'](plan_dir,plan,'install',query=True)==recovered
    try:d['invoke'](plan_dir,plan,'new-with-changed-trust',d['action'](['true']))
    except AssertionError as error:assert 'registered host trust changed' in str(error)
    else:raise AssertionError('new admission accepted changed binding')
    known_hosts.write_bytes(trust)
    # An actual remote authorization revocation is NOT a missing local file.
    auth=root/'authorized_keys';auth.rename(root/'authorized_keys.preserved')
    auth.write_bytes(b'')
    try:d['invoke'](plan_dir,plan,'install',query=True)
    except RuntimeError as error:assert 'unknown' in str(error)
    else:raise AssertionError('query bypassed remote key revocation')
    auth.write_bytes((root/'authorized_keys.preserved').read_bytes())
    assert d['invoke'](plan_dir,plan,'install',query=True)==recovered
    # Restart only our isolated sshd with a DIFFERENT real host key. CAS trust
    # must reject it, even though the mutable local known_hosts path is intact.
    subprocess.run(['ssh-keygen','-q','-t','ed25519','-N','','-f',str(root/'changed_host_key')],check=True)
    original_config=(root/'sshd_config').read_text()
    def restart_fixture(text):
        global server
        server.terminate();server.wait(timeout=5)
        (root/'sshd_config').write_text(text)
        server=subprocess.Popen([str(sshd),'-D','-e','-f',str(root/'sshd_config')],stdout=log,stderr=log,
            env=dict(os.environ,LD_LIBRARY_PATH=str(libdir)))
        for _ in range(100):
            if server.poll() is not None:raise RuntimeError('fixture sshd restart failed')
            try:
                with socket.create_connection(('127.0.0.1',server_port),timeout=0.1):return
            except OSError:time.sleep(0.02)
        raise RuntimeError('fixture sshd restart timed out')
    restart_fixture(original_config.replace('HostKey '+str(root/'host_key'),'HostKey '+str(root/'changed_host_key')))
    try:d['invoke'](plan_dir,plan,'install',query=True)
    except RuntimeError as error:assert 'unknown' in str(error)
    else:raise AssertionError('query accepted changed real host key')
    restart_fixture(original_config)
    assert d['invoke'](plan_dir,plan,'install',query=True)==recovered
    d['deploy'](plan_dir,plan)
    # A full retry queries every old mutating operation, including join; status
    # is intentionally a new read-only observation.
    d['deploy'](plan_dir,plan)
    status=d['node_call'](plan_dir,plan,'status-readback',{'command':'status','operation_id':plan['deployment_id']})
    assert status['metadata']['retired_identities']==[old_self]
    assert status['metadata']['identity']==identity
    assert status['running_manifest']==plan['manifest']
    assert status['capabilities']['smart_node_evidence'] is False
    with sqlite3.connect(root/'effects.sqlite3') as effects:
        count=effects.execute('SELECT count(*) FROM attempts WHERE operation_id=?',[op]).fetchone()[0]
        assert count==1
    # Resume B's install too (not a second subject or a database migration).
    installed_b=d['invoke'](other_dir,other,'install',d['action'](['python3','-I','-c',other['installer'],
        other['remote_root'],other['deployment_id'],other['artifact_sha256'],
        *[other['staging_root']+'/'+n for n in other['parts']]]))
    assert installed_b['sha256']==other['artifact_sha256']
    assert hashlib.sha256(Path(installed_b['artifact']).read_bytes()).hexdigest()==other['artifact_sha256']
    with sqlite3.connect(root/'effects.sqlite3') as effects:
        for candidate in [plan,other]:
            for label in ['root','staging-root',*candidate['parts'],'install']:
                operation=str(uuid.uuid5(uuid.UUID(candidate['deployment_id']),label))
                assert effects.execute('SELECT count(*) FROM attempts WHERE operation_id=?',[operation]).fetchone()[0]==1
    with sqlite3.connect(remote_root/'data/node.sqlite3') as db:
        assert db.execute('SELECT count(*) FROM commands').fetchone()[0]==1
    # Old-format plan fixture: retain the exact deployment/operation identity,
    # but omit schema-2 execution fields. This is NOT an old binary deployment.
    # Query must use original CAS input; all new admissions require a new plan.
    legacy_dir=root/'legacy-plan';legacy_dir.mkdir(mode=0o700)
    legacy={k:v for k,v in plan.items() if k not in
        {'plan_schema','repair_of','staging_root','installer','root_script'}}
    d['dump'](legacy_dir/'plan.json',legacy)
    legacy_bytes=(legacy_dir/'plan.json').read_bytes()
    request_path=root/'legacy-new-request.json'
    d['dump'](request_path,{'command':'initialize','node_id':worker_id,'identity':identity})
    fresh_label='legacy-new-mutation'
    fresh_operation=str(uuid.uuid5(uuid.UUID(legacy['deployment_id']),fresh_label))
    adapter=[sys.executable,'-I',str(repo/'scripts/managed_node.py')]
    rejected_codes=[]
    for args in [('deploy',str(legacy_dir)),('call',str(legacy_dir),fresh_label,str(request_path))]:
        denied=subprocess.run([*adapter,*args],capture_output=True)
        assert denied.returncode!=0 and b'old plan is query-only' in denied.stderr,denied.stderr
        rejected_codes.append(denied.returncode)
    try:d['invoke'](legacy_dir,legacy,fresh_label,d['action'](['true']))
    except ValueError as error:assert 'old plan is query-only' in str(error)
    else:raise AssertionError('shared invoke boundary admitted old plan')
    # No request/receipt side effects precede rejection, even for fresh labels.
    assert {p.name for p in legacy_dir.iterdir()}=={'plan.json'}
    with sqlite3.connect(root/'effects.sqlite3') as effects:
        assert effects.execute('SELECT count(*) FROM attempts WHERE operation_id=?',[fresh_operation]).fetchone()[0]==0
    old_queries=[]
    for _ in range(2):
        observed=subprocess.run([*adapter,'query',str(legacy_dir),'install'],capture_output=True)
        assert observed.returncode==0,observed.stderr
        old_queries.append(json.loads(observed.stdout))
    assert old_queries==[recovered,recovered]
    # Repair is a new explicit deployment, never resealing an old operation.
    repair_config=dict(config,repair_of_plan=str(legacy_dir),initial_identity=identity)
    d['dump'](root/'repair-config.json',repair_config)
    repair_dir=root/'repair-deployment'
    d['prepare'](repair_dir,root/'repair-config.json')
    repair=json.loads((repair_dir/'plan.json').read_bytes())
    assert repair['plan_schema']==2 and repair['deployment_id']!=legacy['deployment_id']
    assert repair['repair_of']=={'deployment_id':legacy['deployment_id'],
        'plan_sha256':hashlib.sha256(legacy_bytes).hexdigest()}
    d['deploy'](repair_dir,repair)
    repair_queries=[d['invoke'](repair_dir,repair,'install',query=True) for _ in range(2)]
    assert repair_queries[0]==repair_queries[1] and repair_queries[0]['state']=='installed'
    assert (legacy_dir/'plan.json').read_bytes()==legacy_bytes
    with sqlite3.connect(root/'effects.sqlite3') as effects:
        repair_operation=str(uuid.uuid5(uuid.UUID(repair['deployment_id']),'install'))
        for operation in [op,repair_operation]:
            assert effects.execute('SELECT count(*) FROM attempts WHERE operation_id=?',[operation]).fetchone()[0]==1
        assert effects.execute('SELECT count(*) FROM attempts WHERE operation_id=?',[fresh_operation]).fetchone()[0]==0
    plan_boundary={'fixture':'schema-1 shape projected from current plan; not old binary deployment',
        'rejected_cli_exit_codes':rejected_codes,'direct_invoke_rejected':True,'fresh_operation_attempts':0,
        'old_query_repeats':2,'old_operation':op,'old_plan_sha256':hashlib.sha256(legacy_bytes).hexdigest(),
        'old_plan_preserved':True,'repair_deployment':repair['deployment_id'],
        'repair_operation':repair_operation,'repair_query_repeats':2,'original_and_repair_attempts':1,
        'repair_lineage':repair['repair_of']}
    print(json.dumps({'plan_boundary':plan_boundary}),flush=True)
    print(json.dumps({'journey':1,'result':'passed','install_operation':op,'attempts':count,'loss':'actual_closed_SSH_stream','identity_adopted':True,'unrelated_db_unchanged':True,'legacy_migration':migration,'service':'one_shot_no_daemon',
        'interleaved_artifacts':2,'query_with_missing_or_unrelated_trust':True,'durable_marker':'decoded_install_and_C07_receipt_plus_artifact_digest',
        'real_key_revocation_and_changed_host_rejected':True}),flush=True)
    # Journey 2: a durable task leaves a result on old owner; query it, explicitly
    # transfer the pure contract, preserve late facts but fence old settlement,
    # execute on the new owner, accept exactly one terminal outcome after reopen.
    node(controller,{'command':'join','operation_id':uid(),'expected_node':controller_id,'identity':identity,'manifest':plan['manifest']})
    node(controller,{'command':'register','node_id':controller_id,'target_id':'c17-local-authority-worker','binding_digest':'sha256:'+'3'*64})
    # This authenticated receiver snapshot has the same registered source, but
    # has not yet received the task assignment. E1 must not depend on lookup.
    early_receiver=root/'early-receiver';early_receiver.mkdir(mode=0o700)
    with sqlite3.connect(controller/'node.sqlite3') as source:
        with sqlite3.connect(early_receiver/'node.sqlite3') as target:source.backup(target)
    payload=list(b'original task bytes\x00with stable identity')
    task_id=uid()
    first=node(controller,{'command':'create','task_id':task_id,'node_id':worker_id,'input':payload})['ticket']
    original=d['node_call'](plan_dir,plan,'first-task',{'command':'execute','ticket':first,'input':payload})['receipt']
    queried=d['node_call'](plan_dir,plan,'original-task-query',{'command':'query','task_id':task_id,'epoch':first['epoch']})
    assert queried['receipt']==original
    unassociated=node(early_receiver,{'command':'accept','from_node':worker_id,'receipt':original})
    assert unassociated['classification']=='unassociated' and not unassociated['accepted']
    early_export=node(early_receiver,{'command':'export_receipts','after':'0','limit':128})
    assert len(early_export['receipts'])==1 and early_export['receipts'][0]['receipt']==original
    assert early_export['receipts'][0]['reason']=='task_missing'
    # A trusted worker's contradictory outer result must be retained in full,
    # not lost with a transcript rejection, and cannot settle the running task.
    contradictory=copy.deepcopy(original);contradictory['output_digest']='blake3:'+'0'*64
    invalid=node(controller,{'command':'accept','from_node':worker_id,'receipt':contradictory})
    assert invalid['classification']=='invalid' and not invalid['accepted']
    invalid_export=node(controller,{'command':'export_receipts','after':'0','limit':128})
    assert invalid_export['receipts'][0]['receipt']==contradictory
    assert invalid_export['receipts'][0]['reason']=='result_transcript_mismatch'
    with sqlite3.connect(controller/'node.sqlite3') as db:
        assert db.execute('SELECT count(*) FROM results').fetchone()[0]==0
    second=node(controller,{'command':'transfer','task_id':task_id,'expected_revision':first['record_revision'],'expected_epoch':first['epoch'],'to_node':controller_id})['ticket']
    stale=node(controller,{'command':'accept','from_node':worker_id,'receipt':original})
    assert stale['status']=='evidence_only_stale_owner' and not stale['accepted']
    current=node(controller,{'command':'execute','ticket':second,'input':payload})['receipt']
    accepted=node(controller,{'command':'accept','from_node':controller_id,'receipt':current})
    assert accepted['accepted']
    duplicate=node(controller,{'command':'accept','from_node':controller_id,'receipt':current})
    assert duplicate['status']=='already_settled' and not duplicate['accepted']
    late=node(controller,{'command':'accept','from_node':worker_id,'receipt':original})
    assert late['status']=='evidence_only_terminal' and not late['accepted']
    restored=node(controller,{'command':'create','task_id':task_id,'node_id':worker_id,'input':payload})
    assert restored['terminal'] and restored['ticket']==second
    receipts=node(controller,{'command':'export_receipts','after':'0','limit':128})
    assert len(receipts['receipts'])==3
    assert receipts['receipts'][0]['receipt']==contradictory and receipts['receipts'][0]['status']=='invalid'
    assert {r['status'] for r in receipts['receipts']}=={'invalid','terminal','settled'}
    exported=node(controller,{'command':'export','after':'0','limit':128})
    assert {e['raw']['node_id'] for e in exported['events']}=={worker_id,controller_id}
    with sqlite3.connect(controller/'node.sqlite3') as db:
        assert db.execute('SELECT count(*) FROM tasks').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM assignments').fetchone()[0]==2
        assert db.execute('SELECT count(*) FROM results').fetchone()[0]==1
        assert db.execute('SELECT count(*) FROM events').fetchone()[0]==2
        task=json.loads(db.execute('SELECT data FROM tasks').fetchone()[0])
        assert task['owner']['id']==identity['branch_id'] and task['state']['status']=='terminal'
    print(json.dumps({'journey':2,'result':'passed','task_id':task_id,'epochs':[first['epoch'],second['epoch']],
        'stale_owner_fenced_at':'authoritative_result_store','terminal_results':1,'raw_source_events':2,'unassociated_result_retained':True,'invalid_full_receipt_retained':True,'receipt_rows':3,
        'boundary':'pure_no_external_effect_contract; old computation not claimed stopped'}),flush=True)
    d['dump'](root/'result.json',{'passed':True,'journeys':2,'publication':publication,'physical_nodes':False,'smart_node_evidence':False,'manifest':plan['manifest'],
        'deployment_id':plan['deployment_id'],'deployment_B':other['deployment_id'],'install_operation':op,'task_id':task_id,'source_events':exported['events'],'receipts':receipts,'unassociated_receipts':early_export,'legacy_migration':migration,'unrelated_db_unchanged':True,'plan_boundary':plan_boundary})
finally:
    stopping.set();listener.close();server.terminate()
    try:server.wait(timeout=5)
    except subprocess.TimeoutExpired:server.kill();server.wait()
    proxy.join(timeout=5)
    for worker in relays:worker.join(timeout=5)
    assert not proxy.is_alive() and not any(worker.is_alive() for worker in relays)
    log.close()
    for closed_port in [server_port,proxy_port]:
        with socket.socket() as probe:
            probe.settimeout(1)
            assert probe.connect_ex(('127.0.0.1',closed_port))==111,'owned listener still reachable'
    (root/'service-stop.json').write_text(json.dumps({'sshd_stopped':server.poll() is not None,
        'proxy_stopped':not proxy.is_alive(),'refused_ports':[server_port,proxy_port]}))
